import logging
import sys
import time
import uuid
from argparse import ArgumentParser, Namespace
from functools import partial
from itertools import batched

import miro_api
import requests
from miro_api.exceptions import ApiException
from miro_api.models import CreateBoardExportRequest

logger = logging.getLogger("miro_export")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter(
        "%(asctime)s:[%(levelname)s]%(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

METADATA_CSV_PATH = 'data/metadata.csv'

def get_failed_boards():
    with open('data/failed_boards.csv', 'rb') as csv_file:
        lines = [l.decode('UTF-8').rstrip() for l in csv_file]
        return lines


def main(parser_args: Namespace):
    org_id = parser_args.org_id
    m_api = miro_api.MiroApi(parser_args.token)
    offset = parser_args.offset
    limit = parser_args.limit
    total = limit
    all_failed_boards = get_failed_boards()
    failed_boards = []
    f_b = ['uXjVP-SRhcE=','o9J_l5EkypE=','o9J_lYJCwSc=','uXjVKT5ZuXY=','uXjVMbFEjGE=','uXjVMhgJ9-8=','uXjVMWsZJfs=','uXjVNGSmcR4=','uXjVNAbOTn4=','uXjVMZgVZQE=','uXjVNFZEbNw=','uXjVOEdJp_c=','uXjVOSk2sX4=', 'o9J_l70Ea54=','o9J_lYKzAU4=','o9J_l22DhTg=','uXjVKENPaT4=','uXjVKmrGFH0=','uXjVM0raekA=','uXjVMgF1wXs=','uXjVMKG5VS0=','uXjVMdM8bL8=','uXjVMFrXRbo=','uXjVM6GPDCg=','uXjVMYk0eOc=','uXjVNs_J-bA=','uXjVOcDNH10=','uXjVOI21jxE=','uXjVO9kw_ck=','uXjVP-sEp_c=','uXjVOnJQMy8=','uXjVOikH3OU=','uXjVP_a8S04=']
    for failed_board in all_failed_boards:
        if failed_board.split(',')[0] in f_b:
            failed_boards.append(failed_board)

    # while offset < total - parser_args.offset:
    #     logger.info(f'Offset = {offset} - {limit}')
    #     boards = run_request_with_retry(partial(m_api.get_boards, offset=str(offset), limit=str(limit)),
    #                                     5, 20, 'Get Boards Data')
    #     total = boards.total
    #     offset += limit
    #     for board in boards.data:
    #         if board.id not in success_boards:
    #             with open('data/failed_boards.csv', "a") as file:
    #                 file.write(f'{board.id},{board.modified_at},{board.name}\n')

    batched_boards = list(batched(failed_boards, parser_args.limit))
    for batch in batched_boards:
        boards_info_dict = {board.split(',')[0]: board for board in batch}
        board_ids = CreateBoardExportRequest.from_dict({'boardIds': list(boards_info_dict.keys())})

        export = run_request_with_retry(partial(begin_export, board_ids, m_api, org_id),
                                        10, 10, 'Begin Boards Export')

        job_id = export.job_id
        run_request_with_retry(partial(check_export_status, job_id, m_api, org_id),
                               60, 180, 'Export Status Check')

        results = run_request_with_retry(partial(m_api.enterprise_board_export_job_results, org_id=org_id, job_id=job_id),
                                         5, 10, 'Export Job Results')

        download_exported_files(results.results, boards_info_dict)


def begin_export(board_ids, m_api, org_id):
    export = m_api.enterprise_create_board_export(org_id=org_id,
                                                  request_id=(str(uuid.uuid4())),
                                                  create_board_export_request=board_ids)
    logger.info(f'Export job id: {export.job_id}')
    return export


def check_export_status(job_id, m_api, org_id):
    status = m_api.enterprise_board_export_job_status(org_id=org_id, job_id=job_id)
    if status.job_status != 'FINISHED':
        raise ApiException(reason=status.job_status)
    return status


def download_exported_files(results, boards_info_dict):
    for result in results:
        board_id = result.board_id
        board = boards_info_dict.get(board_id).split(',')
        if result.status == 'SUCCESS':
            response = run_request_with_retry(partial(requests.get, result.export_link, stream=True),
                                              1, 10, 'Exported File Download')
            with open(f'data/{board_id}.zip', mode="wb") as file:
                for chunk in response.iter_content(chunk_size=10 * 1024):
                    file.write(chunk)
        else:
            logger.error(f'Board: "{board_id}" - failed to export. Error message: "{result.error_message}"')
        with open(METADATA_CSV_PATH, "a") as file:
            file.write(f'{board_id}|{board[1]}|{board[2]}|{result.status}\n')


def run_request_with_retry(request, delay, attempts, description: str):
    for attempt in range(attempts):
        try:
            return request()
        except ApiException as e:
            logger.warning(
                f"Request '{description}' - failed (attempt {attempt + 1}/{attempts}). Reason: %s", e.reason)
            if attempt < attempts - 1:
                logging.info(f"Waiting for {delay} seconds before retrying...")
                time.sleep(delay)
                # delay = min(delay * 2, 300)  # Exponential backoff
            else:
                logging.error("All retries failed")


if __name__ == "__main__":
    parser = ArgumentParser(description='Script to export miro boards')
    parser.add_argument('-i', '--org_id', required=True, default='', dest='org_id',
                        help='organization miro id')
    parser.add_argument('-t', '--token', required=True, default='', dest='token',
                        help='miro app token')
    parser.add_argument('-o', '--offset',  dest='offset', type=int, default=0,
                        help='boards retrieve begin offset')
    parser.add_argument('-l', '--limit', dest='limit',type=int, default=50,
                        help='boards retrieve request limit')
    parser.add_argument('--bid', action='append', dest='miro_board_ids', help="id's of boards")
    parser.add_argument('--resume', dest='resume_from_last', type=bool, default=False,
                        help='resume from last offset')
    args = parser.parse_args()
    main(args)
    exit(0)

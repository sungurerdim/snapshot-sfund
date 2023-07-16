# -*- coding: UTF-8 -*-

import sys
import psutil
import random
import argparse
import pandas as pd
import requests, ctypes

from glob import glob
from web3 import Web3
from time import sleep, time
from datetime import datetime
from natsort import natsorted, ns
from decimal import Decimal, getcontext

from os import system, remove, path, getcwd, chdir, makedirs, getenv, name as osname
from dotenv import load_dotenv

def pressEnter():
    global BLACK_WHITE

    if BLACK_WHITE:
        choice = input("Press Enter to exit")
    else:
        choice = input(cyanLight("Press ") + yellowLight("Enter") + cyanLight(" to exit"))

def start_timer(): return time()

def end_timer(start_time, row_count=None):
    global BLACK_WHITE

    end_time = time()
    execution_time = round(end_time - start_time, 2)

    per_row = None
    if row_count is not None and row_count > 0:
        per_row = round(execution_time / row_count, 4)

    print()
    if BLACK_WHITE:
        print("Execution time:", execution_time, "seconds")
    else:
        print(greenLight("Execution time:"), cyanLight(execution_time), cyanLight("seconds"))

    if per_row is not None:
        if BLACK_WHITE:
            print(str(per_row), "seconds/row")
            print(str(row_count) + " rows in total")
        else:
            print(yellowLight(str(per_row)), yellowLight("seconds/row"))
            print(magentaLight(str(row_count) + " rows in total"))

    print()
    # pressEnter()

def createDir(parentDir, targetDir):
    try:
        dirPath = path.join(parentDir, targetDir)
        if osname == "nt": dirPath = dirPath.replace("\\", "/")

        makedirs(dirPath, exist_ok=True)
        return dirPath
    except OSError as error:
        print(error)

def getRequestResult(_target_URL, _parameters):
    retry_delay = 3
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    while True:
        try:
            response = session.get(_target_URL, params=_parameters, timeout=30)
            response.raise_for_status()
            return response.json()["result"]
        except requests.exceptions.RequestException as ex:
            print()
            print(f"An exception of type {type(ex).__name__} occurred: {ex}")
            print(f"Request response: {response}")
            print(f"Will retry after {retry_delay} seconds...")
            print()
            sleep(retry_delay)
        finally:
            session.close()

def getCurrentDir(): return path.abspath(path.dirname(sys.argv[0]))

def getTxnList(tokenType, ofThisWallet, forThisToken, apiURL, apiKey, startBlock, endBlock):
    module = "account"
    sort_order = "asc"

    action_mapping = {
        "erc721": "tokennfttx",
        "bep721": "tokennfttx",
        "erc1155": "token1155tx",
        "bep20": "tokentx",
    }

    if tokenType not in action_mapping:
        print(f"Invalid token type: {tokenType}")
        return []

    action = action_mapping[tokenType]

    params = {
        "module": module,
        "action": action,
        "address": ofThisWallet,
        "contractaddress": forThisToken,
        "startblock": startBlock,
        "endblock": endBlock,
        "sort": sort_order,
        "apikey": apiKey,
    }

    return getRequestResult(apiURL, params)

def from_wei(amount):
    if not amount:
        raise ValueError("Invalid input: amount cannot be empty or None")

    try:
        amount = Decimal(amount)
    except (TypeError, ValueError):
        raise ValueError("Invalid input: amount must be a valid number")

    epsilon = Decimal(str(10 ** 18))

    return amount / epsilon

def deleteFile(targetFile):
    try:
        remove(targetFile)
    except OSError:
        pass

def saveAsCSV(sourceList, targetDir, targetFileName):
    fullPath = path.join(targetDir, targetFileName)

    sourceDataFrame = pd.DataFrame(sourceList)
    sourceDataFrame.convert_dtypes()

    sourceDataFrame.to_csv(fullPath, index=False, header=False)

def setActiveDir(targetDir=""):
    if not targetDir: targetDir = getcwd()

    chdir(targetDir)
    return targetDir

def setTerminalTitle(title):
    try:
        if sys.platform.startswith('win32') or osname == "nt":
            ctypes.windll.kernel32.SetConsoleTitleW(title)
        else:
            sys.stdout.write(title)
    except Exception as e:
        print(f"Error setting terminal title: {e}")

def show_exception_and_exit(exc_type, exc_value, tb):
    import traceback
    traceback.print_exception(exc_type, exc_value, tb)
    pressEnter()
    sys.exit(-1)

def getTotalTokenAmountOnBlock(forThisToken, onThisBlock, apiURL, apiKey):
    module = "stats"
    action = "tokensupplyhistory"

    params = {
        "module": module,
        "action": action,
        "contractaddress": forThisToken,
        "blockno": onThisBlock,
        "apikey": apiKey
    }

    return from_wei(getRequestResult(apiURL, params))

def getTotalTokenAmountInContract(forThisToken, inThisContract, onThisBlock, apiURL, apiKey):
    module = "account"
    action = "tokenbalancehistory"

    params = {
        "module": module,
        "action": action,
        "contractaddress": forThisToken,
        "address": inThisContract,
        "blockno": onThisBlock,
        "apikey": apiKey
    }

    return from_wei(getRequestResult(apiURL, params))

def getContractOwner(ofThisContract, apiURL, apiKey):
    module = "contract"
    action = "getcontractcreation"

    params = {
        "module": module,
        "action": action,
        "contractaddresses": ofThisContract,
        "sort": "asc",
        "apikey": apiKey
    }

    result = None
    while result is None:
        result = getRequestResult(apiURL, params)

    return result[0]["contractCreator"]

class Color:
    ResetAll = "\033[0m"
    Bold       = "\033[1m"
    Dim        = "\033[2m"
    Underlined = "\033[4m"
    Blink      = "\033[5m"
    Reverse    = "\033[7m"
    Hidden     = "\033[8m"

    ResetBold       = "\033[21m"
    ResetDim        = "\033[22m"
    ResetUnderlined = "\033[24m"
    ResetBlink      = "\033[25m"
    ResetReverse    = "\033[27m"
    ResetHidden     = "\033[28m"

    Default      = "\033[39m"
    Black        = "\033[30m"
    Red          = "\033[31m"
    Green        = "\033[32m"
    Yellow       = "\033[33m"
    Blue         = "\033[34m"
    Magenta      = "\033[35m"
    Cyan         = "\033[36m"
    LightGray    = "\033[37m"
    DarkGray     = "\033[90m"
    LightRed     = "\033[91m"
    LightGreen   = "\033[92m"
    LightYellow  = "\033[93m"
    LightBlue    = "\033[94m"
    LightMagenta = "\033[95m"
    LightCyan    = "\033[96m"
    White        = "\033[97m"

    BackgroundDefault      = "\033[49m"
    BackgroundBlack        = "\033[40m"
    BackgroundRed          = "\033[41m"
    BackgroundGreen        = "\033[42m"
    BackgroundYellow       = "\033[43m"
    BackgroundBlue         = "\033[44m"
    BackgroundMagenta      = "\033[45m"
    BackgroundCyan         = "\033[46m"
    BackgroundLightGray    = "\033[47m"
    BackgroundDarkGray     = "\033[100m"
    BackgroundLightRed     = "\033[101m"
    BackgroundLightGreen   = "\033[102m"
    BackgroundLightYellow  = "\033[103m"
    BackgroundLightBlue    = "\033[104m"
    BackgroundLightMagenta = "\033[105m"
    BackgroundLightCyan    = "\033[106m"
    BackgroundWhite        = "\033[107m"

def white(string: str) -> str:
    return Color.White + str(string) + Color.ResetAll

def redLight(string: str) -> str:
    return Color.LightRed + str(string) + Color.ResetAll

def greenLight(string: str) -> str:
    return Color.LightGreen + str(string) + Color.ResetAll

def blueLight(string: str) -> str:
    return Color.LightBlue + str(string) + Color.ResetAll

def yellowLight(string: str) -> str:
    return Color.LightYellow + str(string) + Color.ResetAll

def magentaLight(string: str) -> str:
    return Color.LightMagenta + str(string) + Color.ResetAll

def cyanLight(string: str) -> str:
    return Color.LightCyan + str(string) + Color.ResetAll

def cyanDark(string: str) -> str:
    return Color.Cyan + str(string) + Color.ResetAll

def clear(): Color.ResetAll; system('cls||clear'); Color.ResetAll; print()

def checkAddress(wallet_): return Web3.to_checksum_address(wallet_)

def epochToBlockNumber(targetEpoch_, closest_ = "after", apiKey_ = ""):
    global BLACK_WHITE

    if not targetEpoch_: return 0

    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": targetEpoch_,
        "closest": closest_,
        "apikey": apiKey_
    }

    while True:
        try:
            response = getRequestResult(API_ENDPOINT, params)
            break
        except (ConnectionError, TimeoutError):
            # API request failed, wait and try again
            if BLACK_WHITE:
                print("API request failed, will try again in 3 seconds.")
            else:
                print(redLight("API request failed, will try again in 3 seconds."))

            sleep(3)

    return int(response)

def set_tier(token_balance_, tier_list_):
    if not token_balance_:
        return "tier0", 0, False

    token_balance_ = Decimal(str(token_balance_))
    include_in_lottery = (250 <= token_balance_ < 500)

    for i, tier in enumerate(tier_list_):
        if token_balance_ >= tier[1]:
            return f"tier{tier[0]}", tier[2], include_in_lottery

    return "tier0", 0, False


def filter_lottery_participants(df):
    tier1_rows = df[(df['Tokens'] >= 250) & (df['Tokens'] < 500)]
    tier1_indices = tier1_rows.index.tolist()
    tier1_data = tier1_rows.values.tolist()
    tier2_rows = df.loc[~df.index.isin(tier1_indices)]

    return tier1_data, tier2_rows

def select_random_half(items):
    half_length = len(items) // 2
    half_items = random.sample(items, k= half_length)

    return half_items

def find_file(name):
    try:
        files = glob(str(name))

        if files:
            return files[0]
    except Exception as e:
        print(f"Error finding file: {e}")

    return None

def calculate_day_difference(from_, until_):
    global DAILY_EPOCH_DIFF

    if from_ > until_:
        raise ValueError("Start date epoch is higher than end date epoch. Check please.")

    day_diff = (int(until_) - int(from_)) // int(DAILY_EPOCH_DIFF)

    return day_diff

def filterTxns(fromThisTxnList_, forThisWallet_):
    forThisWallet_ = forThisWallet_.lower()
    return [txn for txn in fromThisTxnList_ if txn["from"].lower() == forThisWallet_ or txn["to"].lower() == forThisWallet_]

def set_txn_variable(search_for, in_this_txn):
    if not isinstance(in_this_txn, dict):
        raise ValueError("Invalid input: in_this_txn must be a dictionary")

    value = in_this_txn.get(str(search_for))

    if value is None:
        raise KeyError(f"Key '{search_for}' not found in the transaction data")

    if isinstance(value, int):
        return value
    elif isinstance(value, str) and value.isdigit():
        return int(value)
    else:
        return value

def convert_to_date(epoch_):
    return datetime.utcfromtimestamp(epoch_).strftime("%d %b %Y %H:%M")

def calculate_seed_staking_points(stake_balance_, staked_days_, multiplier_):
    DAILY_SSP = ( Decimal(stake_balance_) / Decimal("100") )
    EARNED_SSP_FOR_THIS_SNAPSHOT = ( DAILY_SSP * Decimal(staked_days_) * Decimal(multiplier_) )

    return EARNED_SSP_FOR_THIS_SNAPSHOT

def set_snapshot_timestamp(snapshot_time_=""):
    global DAILY_EPOCH_DIFF

    if snapshot_time_: return int(snapshot_time_)

    utc_now = datetime.utcnow()
    current_epoch_time = int((utc_now - datetime(1970, 1, 1)).total_seconds())

    target_epoch_time = int((utc_now.replace(hour=13, minute=0, second=0, microsecond=0) - datetime(1970, 1, 1)).total_seconds())

    while current_epoch_time <= target_epoch_time:
        target_epoch_time -= DAILY_EPOCH_DIFF

    return int(target_epoch_time)

def remove_old_records(df_, records_to_keep_):
    if len(df_) <= records_to_keep_:
        return df_

    df_ = df_.tail(records_to_keep_)
    return df_

def kill_process(target_file):
    for p in psutil.process_iter():
        try:
            if target_file in str(p.open_files()):
                p.kill()
        except:
            continue

def fetch_lp_history(lp_contract_list):
    global LAST_SNAPSHOT_TIMESTAMP, NEXT_SNAPSHOT_TIMESTAMP, END_TIMESTAMP
    global API_ENDPOINT, API_KEY, EXCLUDE, DAILY_EPOCH_DIFF, approved_wallets
    global SFUND_TOKEN_CONTRACT
    global BLACK_WHITE

    for lp_contract in lp_contract_list:
        setTerminalTitle("Fetching historical LP values for " + lp_contract)

        if BLACK_WHITE:
            print(
                "Fetching historical LP values for", lp_contract, end='\r'
            )
        else:
            print(
                cyanLight("Fetching historical LP values for"), yellowLight(lp_contract), end='\r'
            )

        lp_history_file_name = f"LP_HISTORY_{lp_contract}.csv"
        lp_history_csv = find_file(lp_history_file_name)

        LAST_LP_TIMESTAMP = LAST_SNAPSHOT_TIMESTAMP

        if lp_history_csv:
            DF_LP_HISTORY = pd.read_csv(lp_history_csv, dtype='unicode')
            if not DF_LP_HISTORY.empty:
                LAST_LP_TIMESTAMP = int(DF_LP_HISTORY.iloc[-1]["timeStamp"])
        else:
            DF_LP_HISTORY = pd.DataFrame(columns=['timeStamp','lpAmount','sfundAmount'])

        NEW_LP_VALUES_LIST = []
        NEXT_LP_TIMESTAMP = LAST_LP_TIMESTAMP

        while True:
            NEXT_LP_TIMESTAMP += DAILY_EPOCH_DIFF
            if NEXT_LP_TIMESTAMP > END_TIMESTAMP: break

            NEXT_LP_BLOCK_NUMBER = epochToBlockNumber(NEXT_LP_TIMESTAMP, "after", API_KEY)

            total_lp_amount = getTotalTokenAmountOnBlock(lp_contract, NEXT_LP_BLOCK_NUMBER, API_ENDPOINT, API_KEY)
            total_sfund_amount_in_lps = getTotalTokenAmountInContract(SFUND_TOKEN_CONTRACT, lp_contract, NEXT_LP_BLOCK_NUMBER, API_ENDPOINT, API_KEY)

            NEW_LP_VALUES_LIST.append(
                [ NEXT_LP_TIMESTAMP, Decimal(total_lp_amount), Decimal(total_sfund_amount_in_lps) ]
            )

            if BLACK_WHITE:
                print(
                    "Fetched historical LP values for", convert_to_date(NEXT_LP_TIMESTAMP), "                            ", end='\r'
                )
            else:
                print(
                    cyanLight("Fetched historical LP values for"), white(convert_to_date(NEXT_LP_TIMESTAMP)), "                            ", end='\r'
                )

        if NEW_LP_VALUES_LIST:
            DF_NEW_LP_HISTORY = pd.DataFrame(NEW_LP_VALUES_LIST, columns=['timeStamp','lpAmount','sfundAmount'])
            DF_LP_HISTORY = pd.concat([DF_LP_HISTORY, DF_NEW_LP_HISTORY])

            if len(DF_LP_HISTORY) > 90:
                DF_LP_HISTORY = remove_old_records(DF_LP_HISTORY, 90)

            DF_LP_HISTORY['timeStamp'] = DF_LP_HISTORY['timeStamp'].astype('int')
            DF_LP_HISTORY['lpAmount'] = DF_LP_HISTORY['lpAmount'].astype('float')
            DF_LP_HISTORY['sfundAmount'] = DF_LP_HISTORY['sfundAmount'].astype('float')

            if lp_history_csv:
                kill_process(lp_history_csv)
                deleteFile(lp_history_csv)

            DF_LP_HISTORY.to_csv(lp_history_file_name, index=False)

        if BLACK_WHITE:
            print(
                "Found", len(NEW_LP_VALUES_LIST), "new LP values for", lp_contract
            )
        else:
            print(
                greenLight("Found"), white(len(NEW_LP_VALUES_LIST)), greenLight("new LP values for"), yellowLight(lp_contract)
            )

def fetch_pool_txns(pools, token_to_check, tokenType):
    global BLACK_WHITE
    global END_BLOCK_NUMBER

    token_to_check = token_to_check.strip().lower()

    for pool in pools:
        pool_name = pool[0]
        pool_contract = pool[1].strip().lower()

        # ------------------------------------------------------------------

        txn_export_file_name = f"{pool_contract}.csv"
        txn_export_csv = find_file(txn_export_file_name)

        if txn_export_csv:
            DF_POOL_TXN_HISTORY = pd.read_csv(txn_export_csv, dtype='unicode')[["blockNumber", "timeStamp", "from", "to", "value"]]

            LAST_BLOCK_NUMBER = int(DF_POOL_TXN_HISTORY.iloc[-1]["blockNumber"])
            NEXT_BLOCK_NUMBER = LAST_BLOCK_NUMBER + 1
        else:
            DF_POOL_TXN_HISTORY = pd.DataFrame()
            LAST_BLOCK_NUMBER = 0
            NEXT_BLOCK_NUMBER = LAST_BLOCK_NUMBER

        setTerminalTitle("Fetching transactions for " + pool_name)

        if BLACK_WHITE:
            print(
                "Fetching transactions for", pool_name, pool_contract, end='\r'
            )
        else:
            print(
                cyanLight("Fetching transactions for"), yellowLight(pool_name), white(pool_contract), end='\r'
            )

        NEW_POOL_TXNS = []

        while True:
            partial_txn_list = []
            if NEXT_BLOCK_NUMBER > END_BLOCK_NUMBER: break

            partial_txn_list = getTxnList(tokenType, pool_contract, token_to_check, API_ENDPOINT, API_KEY, NEXT_BLOCK_NUMBER, END_BLOCK_NUMBER)

            if not partial_txn_list: break

            NEW_POOL_TXNS += partial_txn_list

            if BLACK_WHITE:
                print(
                    "Fetched", len(NEW_POOL_TXNS), "txns for", pool_name, pool_contract, end='\r'
                )
            else:
                print(
                    cyanLight("Fetched"), white(len(NEW_POOL_TXNS)), yellowLight("txns for"), yellowLight(pool_name), white(pool_contract), end='\r'
                )

            LAST_BLOCK_NUMBER = int(partial_txn_list[-1]["blockNumber"])
            NEXT_BLOCK_NUMBER = LAST_BLOCK_NUMBER + 1

        if BLACK_WHITE:
            print(
                "Found", len(NEW_POOL_TXNS), "new transactions for", pool_name, pool_contract
            )
        else:
            print(
                greenLight("Found"), white(len(NEW_POOL_TXNS)), greenLight("new transactions for"), yellowLight(pool_name), white(pool_contract)
            )

        if NEW_POOL_TXNS:
            DF_NEW_POOL_TXNS = pd.DataFrame(NEW_POOL_TXNS)[["blockNumber", "timeStamp", "from", "to", "value"]]

            DF_POOL_TXN_HISTORY = pd.concat([DF_POOL_TXN_HISTORY, DF_NEW_POOL_TXNS])

            DF_POOL_TXN_HISTORY['blockNumber'] = DF_POOL_TXN_HISTORY['blockNumber'].astype('int')
            DF_POOL_TXN_HISTORY['timeStamp'] = DF_POOL_TXN_HISTORY['timeStamp'].astype('int')
            DF_POOL_TXN_HISTORY['value'] = DF_POOL_TXN_HISTORY['value'].astype('float')

            if txn_export_csv:
                kill_process(txn_export_csv)
                deleteFile(txn_export_csv)

            DF_POOL_TXN_HISTORY.to_csv(txn_export_file_name, index=False)

def print_percent_done(title, index, total, bar_len=50):
    global BLACK_WHITE

    percent_done = (index + 1) / total * 100
    done = int(percent_done * bar_len / 100)

    togo = bar_len - done

    done_str = '█' * done
    togo_str = '░' * togo

    if BLACK_WHITE:
        progress_bar = f'{done_str}{togo_str}'
        progress_info = f'{title}: {progress_bar} {f"{percent_done:.2f}"}{"% done"}'
    else:
        progress_bar = f'{greenLight(done_str)}{togo_str}'
        progress_info = f'{yellowLight(title)}: {progress_bar} {magentaLight(f"{percent_done:.2f}")}{magentaLight("% done")}'

    if index != (total - 1):
        print(progress_info, end='\r', flush=True)
    else:
        completion_mark = '✅'
        # completion_mark = greenLight('✓')

        progress_info += f' {completion_mark}'

        print(progress_info, flush=True)

def update_balance(current_balance_, balance_change_):
    current_balance_ += Decimal(balance_change_)

    return Decimal("0") if current_balance_ < 0 else current_balance_

def process_stake_txns():
    global results_header, results_dict, unique_wallets, POOL_TXN_HISTORY
    global pool_name, pool_contract, pool_type, SEED_STAKING_POINT_MULTIPLIER
    global SEED_STAKING_START_TIMESTAMP, END_TIMESTAMP, DAILY_EPOCH_DIFF

    unique_wallet_count = len(unique_wallets)

    for uw_index, uw in enumerate(unique_wallets):
        print_percent_done(pool_name, uw_index, unique_wallet_count)

        NEXT_SNAPSHOT_TIMESTAMP = int( SEED_STAKING_START_TIMESTAMP )
        LAST_SNAPSHOT_TIMESTAMP = int( SEED_STAKING_START_TIMESTAMP - (1 * DAILY_EPOCH_DIFF) )

        uw_txns = filterTxns(POOL_TXN_HISTORY, uw)
        uw_txns_count = len(uw_txns)

        if uw_txns_count == 0: continue

        empty_dict_item = {
            "Wallet": uw,
            **{key: uw for key in results_header[:1]},
            **{key: Decimal("0") for key in results_header[1:]}
        }
        result = results_dict.get(str(uw), empty_dict_item)

        SFUND_BALANCE_OF_WALLET = Decimal(result[ pool_name ])

        EARNED_SSP = Decimal(result.get("SSP", "0"))
        SNAPSHOT_BALANCE = Decimal("0")

        for uw_txn in uw_txns:
            txnTimestamp = int(uw_txn["timeStamp"])
            if txnTimestamp > END_TIMESTAMP: break

            txnFrom = set_txn_variable("from", uw_txn).strip().lower()
            txnTo = set_txn_variable("to", uw_txn).strip().lower()
            txnValue = from_wei(set_txn_variable("value", uw_txn))

            token_diff = txnValue

            if txnTo == pool_contract:
                txnType = "Stake"
            elif txnFrom == pool_contract:
                txnType = "Withdraw"
                token_diff = token_diff * (-1)
            else:
                print()
                print("There is a logical error.")
                print("Pool:", pool_contract, "Timestamp:", txnTimestamp, "From:", txnFrom, "To:", txnTo, "Value:", token_diff)
                print()

                sys.exit(1)

            txn_diff_included = False

            if txnTimestamp < NEXT_SNAPSHOT_TIMESTAMP:
                SFUND_BALANCE_OF_WALLET = update_balance(SFUND_BALANCE_OF_WALLET, token_diff)
                txn_diff_included = True

                continue
            elif txnTimestamp == NEXT_SNAPSHOT_TIMESTAMP:
                SFUND_BALANCE_OF_WALLET = update_balance(SFUND_BALANCE_OF_WALLET, token_diff)
                txn_diff_included = True

            if txnTimestamp >= NEXT_SNAPSHOT_TIMESTAMP:
                SNAPSHOT_BALANCE = SFUND_BALANCE_OF_WALLET
                DAYS_PASSED_SINCE_PREVIOUS_SNAPSHOT = calculate_day_difference(LAST_SNAPSHOT_TIMESTAMP, txnTimestamp)

                EARNED_SSP += calculate_seed_staking_points(
                        SNAPSHOT_BALANCE,
                        DAYS_PASSED_SINCE_PREVIOUS_SNAPSHOT,
                        SEED_STAKING_POINT_MULTIPLIER
                    )

                LAST_SNAPSHOT_TIMESTAMP += ( DAILY_EPOCH_DIFF * DAYS_PASSED_SINCE_PREVIOUS_SNAPSHOT )
                NEXT_SNAPSHOT_TIMESTAMP = ( LAST_SNAPSHOT_TIMESTAMP + DAILY_EPOCH_DIFF )

            if not txn_diff_included:
                SFUND_BALANCE_OF_WALLET = update_balance(SFUND_BALANCE_OF_WALLET, token_diff)
                txn_diff_included = True

            # ------------------------------------------------------------------

        if NEXT_SNAPSHOT_TIMESTAMP <= END_TIMESTAMP:
            SNAPSHOT_BALANCE = SFUND_BALANCE_OF_WALLET
            DAYS_PASSED_SINCE_PREVIOUS_SNAPSHOT = calculate_day_difference(LAST_SNAPSHOT_TIMESTAMP, END_TIMESTAMP)

            EARNED_SSP += calculate_seed_staking_points(
                    SNAPSHOT_BALANCE,
                    DAYS_PASSED_SINCE_PREVIOUS_SNAPSHOT,
                    SEED_STAKING_POINT_MULTIPLIER
                )

            LAST_SNAPSHOT_TIMESTAMP += DAILY_EPOCH_DIFF
            NEXT_SNAPSHOT_TIMESTAMP = ( LAST_SNAPSHOT_TIMESTAMP + DAILY_EPOCH_DIFF )

        result[pool_name] = SNAPSHOT_BALANCE
        result["SSP"] = EARNED_SSP
        results_dict[uw] = result

def process_farm_txns():
    global results_dict, lp_history_dict, unique_wallets, POOL_TXN_HISTORY
    global pool_name, pool_contract, pool_type, SEED_STAKING_POINT_MULTIPLIER
    global SEED_STAKING_START_TIMESTAMP, END_TIMESTAMP, DAILY_EPOCH_DIFF

    unique_wallet_count = len(unique_wallets)

    for uw_index, uw in enumerate(unique_wallets):
        print_percent_done(pool_name, uw_index, unique_wallet_count)

        NEXT_SNAPSHOT_TIMESTAMP = int( SEED_STAKING_START_TIMESTAMP )
        LAST_SNAPSHOT_TIMESTAMP = int( SEED_STAKING_START_TIMESTAMP - (1 * DAILY_EPOCH_DIFF) )

        uw_txns = filterTxns(POOL_TXN_HISTORY, uw)
        uw_txns_count = len(uw_txns)

        if uw_txns_count == 0: continue

        empty_dict_item = {
            "Wallet": uw,
            **{key: uw for key in results_header[:1]},
            **{key: Decimal("0") for key in results_header[1:]}
        }
        result = results_dict.get(str(uw), empty_dict_item)

        LP_BALANCE_OF_WALLET = Decimal(result.get("LP (" + pool_name + ")", "0"))

        EARNED_SSP = Decimal(result.get("SSP", "0"))
        SNAPSHOT_BALANCE = Decimal("0")

        for uw_txn in uw_txns:
            txnTimestamp = int(uw_txn["timeStamp"])
            if txnTimestamp > END_TIMESTAMP: break

            txnFrom = set_txn_variable("from", uw_txn).strip().lower()
            txnTo = set_txn_variable("to", uw_txn).strip().lower()
            txnValue = from_wei(set_txn_variable("value", uw_txn))

            token_diff = txnValue

            if txnTo == pool_contract:
                txnType = "Stake"
            elif txnFrom == pool_contract:
                txnType = "Withdraw"
                token_diff = token_diff * (-1)
            else:
                print()
                print("There is a logical error.")
                print("Pool:", pool_contract, "Timestamp:", txnTimestamp, "From:", txnFrom, "To:", txnTo, "Value:", token_diff)
                print()

                sys.exit(1)

            txn_diff_included = False

            if txnTimestamp < NEXT_SNAPSHOT_TIMESTAMP:
                LP_BALANCE_OF_WALLET = update_balance(LP_BALANCE_OF_WALLET, token_diff)
                txn_diff_included = True

                continue
            elif txnTimestamp == NEXT_SNAPSHOT_TIMESTAMP:
                LP_BALANCE_OF_WALLET = update_balance(LP_BALANCE_OF_WALLET, token_diff)
                txn_diff_included = True

            while txnTimestamp >= NEXT_SNAPSHOT_TIMESTAMP:
                LP_VALUES = lp_history_dict.get(str(NEXT_SNAPSHOT_TIMESTAMP))
                if not LP_VALUES: raise ValueError("LP_VALUES is empty or non-existent")

                TOTAL_LP_TOKENS = LP_VALUES.get("lpAmount")
                if not TOTAL_LP_TOKENS: raise ValueError("TOTAL_LP_TOKENS is empty or non-existent")
                TOTAL_LP_TOKENS = Decimal(TOTAL_LP_TOKENS)

                TOTAL_SFUND_TOKENS_IN_LP_TOKENS = LP_VALUES.get("sfundAmount")
                if not TOTAL_SFUND_TOKENS_IN_LP_TOKENS: raise ValueError("TOTAL_SFUND_TOKENS_IN_LP_TOKENS is empty or non-existent")
                TOTAL_SFUND_TOKENS_IN_LP_TOKENS = Decimal(TOTAL_SFUND_TOKENS_IN_LP_TOKENS)

                # LP tokens of wallet / Total LP tokens = Wallet's pool share
                # Wallet's pool share * Total SFUND tokens in LP tokens = Wallet's SFUND token amount

                POOL_SHARE_OF_WALLET = LP_BALANCE_OF_WALLET / TOTAL_LP_TOKENS
                SNAPSHOT_BALANCE = POOL_SHARE_OF_WALLET * TOTAL_SFUND_TOKENS_IN_LP_TOKENS

                DAYS_PASSED_SINCE_PREVIOUS_SNAPSHOT = 1

                EARNED_SSP += calculate_seed_staking_points(
                        SNAPSHOT_BALANCE,
                        DAYS_PASSED_SINCE_PREVIOUS_SNAPSHOT,
                        SEED_STAKING_POINT_MULTIPLIER
                    )

                LAST_SNAPSHOT_TIMESTAMP += DAILY_EPOCH_DIFF
                NEXT_SNAPSHOT_TIMESTAMP = ( LAST_SNAPSHOT_TIMESTAMP + DAILY_EPOCH_DIFF )

            if not txn_diff_included:
                LP_BALANCE_OF_WALLET = update_balance(LP_BALANCE_OF_WALLET, token_diff)
                txn_diff_included = True

            # ------------------------------------------------------------------

        while NEXT_SNAPSHOT_TIMESTAMP <= END_TIMESTAMP:
            LP_VALUES = lp_history_dict.get(str(NEXT_SNAPSHOT_TIMESTAMP))

            if not LP_VALUES: raise ValueError("LP_VALUES is empty or non-existent")

            TOTAL_LP_TOKENS = LP_VALUES.get("lpAmount")
            if not TOTAL_LP_TOKENS: raise ValueError("TOTAL_LP_TOKENS is empty or non-existent")
            TOTAL_LP_TOKENS = Decimal(TOTAL_LP_TOKENS)

            TOTAL_SFUND_TOKENS_IN_LP_TOKENS = LP_VALUES.get("sfundAmount")
            if not TOTAL_SFUND_TOKENS_IN_LP_TOKENS: raise ValueError("TOTAL_SFUND_TOKENS_IN_LP_TOKENS is empty or non-existent")
            TOTAL_SFUND_TOKENS_IN_LP_TOKENS = Decimal(TOTAL_SFUND_TOKENS_IN_LP_TOKENS)

            # LP tokens of wallet / Total LP tokens = Wallet's pool share
            # Wallet's pool share * Total SFUND tokens in LP tokens = Wallet's SFUND token amount

            POOL_SHARE_OF_WALLET = LP_BALANCE_OF_WALLET / TOTAL_LP_TOKENS
            SNAPSHOT_BALANCE = POOL_SHARE_OF_WALLET * TOTAL_SFUND_TOKENS_IN_LP_TOKENS

            DAYS_PASSED_SINCE_PREVIOUS_SNAPSHOT = 1

            EARNED_SSP += calculate_seed_staking_points(
                    SNAPSHOT_BALANCE,
                    DAYS_PASSED_SINCE_PREVIOUS_SNAPSHOT,
                    SEED_STAKING_POINT_MULTIPLIER
                )

            LAST_SNAPSHOT_TIMESTAMP += DAILY_EPOCH_DIFF
            NEXT_SNAPSHOT_TIMESTAMP = ( LAST_SNAPSHOT_TIMESTAMP + DAILY_EPOCH_DIFF )

        result["LP (" + pool_name + ")"] = LP_BALANCE_OF_WALLET
        result["SFUND (" + pool_name + ")"] = SNAPSHOT_BALANCE
        result["SSP"] = EARNED_SSP
        results_dict[uw] = result

def read_lp_history(TARGET_LP_CONTRACT_):
    lp_history_file = find_file(f"LP_HISTORY_{TARGET_LP_CONTRACT_}.csv")

    if not lp_history_file: return {}

    DF_LP_HISTORY = pd.read_csv(lp_history_file, dtype='unicode')

    return {record['timeStamp']: record for record in DF_LP_HISTORY.to_dict('records')}

# ------------------------------------------------------------------

argParser = argparse.ArgumentParser()
argParser.add_argument("-e", "--env", help="Specify env variables file")
argParser.add_argument("-i", "--include", help="Set snapshot inclusion period in days")
argParser.add_argument("-nc", "--no-color", action="store_true", help="Disable color output")
argParser.add_argument("-st", "--start-time", help="Set start time epoch")
argParser.add_argument("-et", "--end-time", help="Set end time epoch")
argParser.add_argument("-d", "--data-dir", help="Path to save transaction exports of contracts as CSV files")
argParser.add_argument("-o", "--output-dir", help="Path to save snapshot CSV file")
argParser.add_argument("-of", "--output-filename", help="Preferred name for snapshot CSV file")

args = argParser.parse_args()

env_file = args.env or ".env"

if not path.exists(env_file):
    raise FileNotFoundError(f"The env file ({env_file}) is missing.")

BLACK_WHITE = args.no_color

# ------------------------------------------------------------------

sys.excepthook = show_exception_and_exit

clear()

currentDir = setActiveDir(getCurrentDir())

load_dotenv(env_file)

API_ENDPOINT = getenv("API_ENDPOINT")
API_KEY = getenv("API_KEY")

TIERS = getenv("TIERS")
TIER_LIMITS = getenv("TIER_LIMITS")
POOL_WEIGHTS = getenv("POOL_WEIGHTS")

EXCLUDE = getenv("EXCLUDE")

DATA_DIR = args.data_dir or "Data"

OUTPUT_DIR = args.output_dir or "Output"
OUTPUT_FILENAME = args.output_filename or "Snapshot.csv"
if not ".csv" in OUTPUT_FILENAME: OUTPUT_FILENAME = f"{OUTPUT_FILENAME}.csv"

EXCLUDE = list(map(str, EXCLUDE.split(",")))
EXCLUDE = [ ex.strip().lower() for ex in EXCLUDE ]

tiers = list(map(int, TIERS.split(",")))
tier_limits = list(map(int, TIER_LIMITS.split(",")))
pool_weights = list(map(float, POOL_WEIGHTS.split(",")))

tier_list = list(zip(tiers, tier_limits, pool_weights))

dataDir = createDir(currentDir, DATA_DIR)
outputDir = createDir(currentDir, OUTPUT_DIR)

# ------------------------------------------------------------------

token_type = "bep20"
TOKEN_DECIMAL_PLACES = 18

SFUND_TOKEN_CONTRACT = '0x477bC8d23c634C154061869478bce96BE6045D12'
SFUND_TOKEN_CONTRACT = checkAddress(SFUND_TOKEN_CONTRACT)

SNFTS_TOKEN_CONTRACT = '0x6f51A1674BEFDD77f7ab1246b83AdB9f13613762'
SNFTS_TOKEN_CONTRACT = checkAddress(SNFTS_TOKEN_CONTRACT)

LP_CONTRACT_SFUND_BNB = '0x74fA517715C4ec65EF01d55ad5335f90dce7CC87'
LP_CONTRACT_SFUND_BNB = checkAddress(LP_CONTRACT_SFUND_BNB)

SFUND_FARM_CONTRACTS = [
    [ "SFUND_FARM_LEGACY", '0x1f10564bad9367cff4247a138ebba9a9aaeb789e', 1 ],
    [ "SFUND_FARM_NEW", '0x71d058369D39a8488D8e9F5FD5B050610ca788C0', 2 ]
]

SFUND_STAKE_CONTRACTS = [
    [ "SFUND_STAKE_7_DAYS_LEGACY", '0xb667c499b88AC66899E54e27Ad830d423d9Fba69', 1 ],
    [ "SFUND_STAKE_14_DAYS_LEGACY", '0x027fC3A49383D0E7Bd6b81ef6C7512aFD7d22a9e', 1 ],
    [ "SFUND_STAKE_30_DAYS_LEGACY", '0x8900475BF7ed42eFcAcf9AE8CfC24Aa96098f776', 1 ],
    [ "SFUND_STAKE_60_DAYS_LEGACY", '0x66b8c1f8DE0574e68366E8c4e47d0C8883A6Ad0b', 1 ],
    [ "SFUND_STAKE_90_DAYS_LEGACY", '0x5745b7E077a76bE7Ba37208ff71d843347441576', 1 ],
    [ "SFUND_STAKE_180_DAYS_LEGACY", '0xf420F0951F0F50f50C741f6269a4816985670054', 1 ],

    [ "SFUND_STAKE_30_DAYS_NEW", '0x60b9F788F4436f0B5c33785b3499b2ee1D8dbFd4', 0.5 ],
    [ "SFUND_STAKE_90_DAYS_NEW", '0x5b384955ac3460c996402Bf03736624A33e55273', 1 ],
    [ "SFUND_STAKE_180_DAYS_NEW", '0xd01650999BB5740F9bb41168401e9664B28FF47f', 2 ],
    [ "SFUND_STAKE_270_DAYS_NEW", '0x89aaaB217272C89dA91825D9Effbe65dEd384859', 3 ]
]

LP_CONTRACT_SNFTS_SFUND = '0xe4399d0c968fBc3f5449525146ea98B0dC7Fc203'
SNFTS_FARM_CONTRACTS = [
    [ "SNFTS_FARM", "0x19ee35c5B2CcaBaAE367B6f99b2f5747E6a6C0d0", 2 ]
]

# ------------------------------------------------------------------

START_TIMESTAMP = 0
DAILY_EPOCH_DIFF = 86400

SNAPSHOT_INCLUSION_PERIOD = int(args.include or 90)

END_TIMESTAMP = set_snapshot_timestamp(args.end_time)

# ------------------------------------------------------------------

getcontext().prec = TOKEN_DECIMAL_PLACES

if args.start_time:
    SEED_STAKING_START_TIMESTAMP = int(args.start_time)

    if args.end_time:
        SNAPSHOT_INCLUSION_PERIOD = calculate_day_difference(SEED_STAKING_START_TIMESTAMP, int(args.end_time))

else:
    SEED_STAKING_START_TIMESTAMP = int( END_TIMESTAMP - ( (SNAPSHOT_INCLUSION_PERIOD - 1) * DAILY_EPOCH_DIFF) )

NEXT_SNAPSHOT_TIMESTAMP = int( SEED_STAKING_START_TIMESTAMP )
LAST_SNAPSHOT_TIMESTAMP = int( SEED_STAKING_START_TIMESTAMP - (1 * DAILY_EPOCH_DIFF) )

END_BLOCK_NUMBER = epochToBlockNumber(END_TIMESTAMP, "after", API_KEY)

start_date_utc = convert_to_date(SEED_STAKING_START_TIMESTAMP)
end_date_utc = convert_to_date(END_TIMESTAMP)

if BLACK_WHITE:
    print(
        "Snapshot Period:",
        start_date_utc, "-",
        end_date_utc,
        "(",
        SNAPSHOT_INCLUSION_PERIOD,
        "days",
        ")"
    )
    print()
    print("Tiers:", TIERS)
    print("Tier Limits:", TIER_LIMITS)
    print("Pool Weights:", POOL_WEIGHTS)
    print()
    print("Data Directory:", DATA_DIR)
    print("Output Directory:", OUTPUT_DIR)
    print("Output File:", OUTPUT_FILENAME)
else:
    print(
        greenLight("Snapshot Period:"),
        magentaLight(start_date_utc),
        yellowLight("-"),
        magentaLight(end_date_utc),
        cyanLight("("),
        white(SNAPSHOT_INCLUSION_PERIOD),
        white("days"), cyanLight(")")
    )
    print()
    print(cyanLight("Tiers:"), white(TIERS))
    print(cyanLight("Tier Limits:"), white(TIER_LIMITS))
    print(cyanLight("Pool Weights:"), white(POOL_WEIGHTS))
    print()
    print(cyanLight("Data Directory:"), white(DATA_DIR))
    print(cyanLight("Output Directory:"), white(OUTPUT_DIR))
    print(cyanLight("Output File:"), white(OUTPUT_FILENAME))

# ------------------------------------------------------------------

approved_wallets = []

# ------------------------------------------------------------------

startTime = start_timer()

processed_txn_count = 0

results = []

results_header = [ "Wallet", "Include", "Tier", "Pool Weight", "Total SFUND", "SSP", "SSP %" ]

ssp_column_index = [index for index, col in enumerate(results_header) if col == "SSP"][0]
ssp_percent_column_index = [index for index, col in enumerate(results_header) if col == "SSP %"][0]

pools = SFUND_STAKE_CONTRACTS + SFUND_FARM_CONTRACTS + SNFTS_FARM_CONTRACTS
for ind,pool in enumerate(pools):
    if ind < len(SFUND_STAKE_CONTRACTS):
        results_header.append(pool[0])
    else:
        results_header.append("LP (" + pool[0] + ")")
        results_header.append("SFUND (" + pool[0] + ")")

print()

if BLACK_WHITE:
    print("Fetching data...")
    print("-------------------------")
else:
    print(
        magentaLight("Fetching data...")
    )
    print(white("-------------------------"))

currentDir = setActiveDir(dataDir)

fetch_lp_history( [ LP_CONTRACT_SFUND_BNB, LP_CONTRACT_SNFTS_SFUND ] )
fetch_pool_txns( SFUND_FARM_CONTRACTS, LP_CONTRACT_SFUND_BNB, token_type )
fetch_pool_txns( SNFTS_FARM_CONTRACTS, LP_CONTRACT_SNFTS_SFUND, token_type )
fetch_pool_txns( SFUND_STAKE_CONTRACTS, SFUND_TOKEN_CONTRACT, token_type )

if BLACK_WHITE:
    print("-------------------------")
    print("Fetching is complete.")
else:
    print(white("-------------------------"))
    print(
        magentaLight("Fetching is complete.")
    )

print()

currentDir = setActiveDir(outputDir)

results_dict = {}
empty_value = Decimal("0")
currentDir = setActiveDir(dataDir)

for pool in pools:
    pool_name = pool[0]
    pool_contract = pool[1].strip().lower()
    SEED_STAKING_POINT_MULTIPLIER = pool[2]

    # ------------------------------------------------------------------

    txn_export_file = find_file(f"{pool_contract}.csv")
    if not txn_export_file: continue

    # ------------------------------------------------------------------

    if "SFUND_FARM" in pool_name:
        lp_history_dict = read_lp_history(LP_CONTRACT_SFUND_BNB)
        pool_type = "farm"
    elif "SNFTS_FARM" in pool_name:
        lp_history_dict = read_lp_history(LP_CONTRACT_SNFTS_SFUND)
        pool_type = "farm"
    else:
        lp_history_dict = {}
        pool_type = "stake"

    # ------------------------------------------------------------------

    contract_owner = getContractOwner(checkAddress(pool_contract), API_ENDPOINT, API_KEY).strip().lower()
    if not contract_owner in EXCLUDE: EXCLUDE.append(contract_owner)

    DF_POOL_TXN_HISTORY = pd.read_csv(txn_export_file, dtype='unicode')[["blockNumber", "timeStamp", "from", "to", "value"]]

    DF_POOL_TXN_HISTORY["from"] = DF_POOL_TXN_HISTORY["from"].str.lower()
    DF_POOL_TXN_HISTORY["to"] = DF_POOL_TXN_HISTORY["to"].str.lower()

    DF_POOL_TXN_HISTORY = DF_POOL_TXN_HISTORY[
        (~DF_POOL_TXN_HISTORY["from"].isin(EXCLUDE)) &
        (~DF_POOL_TXN_HISTORY["to"].isin(EXCLUDE))
    ]

    if approved_wallets:
        DF_POOL_TXN_HISTORY = DF_POOL_TXN_HISTORY[
            DF_POOL_TXN_HISTORY["from"].isin(approved_wallets) |
            DF_POOL_TXN_HISTORY["to"].isin(approved_wallets)
        ]

    unique_wallets = sorted(natsorted(
                                    list(set(DF_POOL_TXN_HISTORY[["from", "to"]].values.ravel())),
                                    key=str,
                                    alg=ns.IGNORECASE
                                ))

    unique_wallets.remove(pool_contract)
    if not unique_wallets: continue

    POOL_TXN_HISTORY = DF_POOL_TXN_HISTORY.to_dict('records')

    # ------------------------------------------------------------------

    if pool_type == "stake":
        process_stake_txns()
    elif pool_type == "farm":
        process_farm_txns()

header = list(next(iter(results_dict.values())).keys())
results = [list(item.values()) for item in results_dict.values()]

header_length = len(header)

# ------------------------------------------------------------------

sfund_column_indexes = [index for index, col in enumerate(header) if col.startswith("SFUND")]
lottery_participants = []

total_ssp = sum(Decimal(str(row[ssp_column_index])) for row in results)

for row_index, row in enumerate(results):
    total_sfund = sum(Decimal(str(row[ind])) for ind in sfund_column_indexes)
    ssp_percent = row[ssp_column_index] / total_ssp

    tier, weight, lottery_participant = set_tier(total_sfund, tier_list)

    if tier == "tier0":
        row[1] = False
    elif tier == "tier1":
        if lottery_participant:
            row[1] = False
            lottery_participants.append(row[0])
        else:
            row[1] = True
    else:
        row[1] = True

    row[2] = tier
    row[3] = weight
    row[4] = total_sfund
    row[ssp_percent_column_index] = ssp_percent

    results[row_index] = row

# ------------------------------------------------------------------

lottery_participants = list(set(lottery_participants))
lottery_winners = select_random_half(lottery_participants)

for row_index, row in enumerate(results):
    if not row[0] in lottery_winners: continue
    results[row_index][1] = True

# ------------------------------------------------------------------

results = [ header ] + results

currentDir = setActiveDir(outputDir)

snapshot_file = find_file(OUTPUT_FILENAME)

if snapshot_file:
    kill_process(snapshot_file)
    deleteFile(snapshot_file)

saveAsCSV(results, currentDir, OUTPUT_FILENAME)
end_timer(startTime, processed_txn_count)

# ------------------------------------------------------------------

# if __name__ == '__main__':
#     main()
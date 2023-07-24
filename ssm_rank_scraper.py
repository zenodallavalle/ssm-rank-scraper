import json
from multiprocessing import cpu_count
from datetime import datetime
import numpy as np
import openpyxl
import os
import pandas as pd
import time

import grabber
from year_parser import parse_year_long


YEAR = "2018"
COMPUTE_MIN_PTS = False

SHEET_NAME = datetime.now().strftime(
    "%Y-%m-%d-%H-%M-%S"
)  # default is current year-month-day-hours-minutes-seconds

SAVE = True
SKIP_IF_EQUAL_TO_LAST = True

WORKERS = None  # if workers = None processes used will be equal to number of cores, override if needed.


def load_credentials(year):
    with open("credentials.json", "r") as f:
        cred = json.load(f)
        email = cred.get("email", None)

        password = cred.get("password", None)

    authentication_link = cred.get(
        "authentication_link_{}".format(parse_year_long(year)), None
    )

    if authentication_link is None:
        if email is None:
            raise KeyError("email must be present")
        if password is None:
            raise KeyError("password must be present")

    if authentication_link is None:
        print(
            "Authentication_link was not found. For this time we will login with email and password, then we will retrieve authentication link and save it in credentials.json for next times. Using authentication_link can save ~5 seconds."
        )
        print(
            "Authentication link is strictly personal, don't share it with anyone! Also, make sure to delete authetication.json from the folder before sharing the folder with anyone."
        )
        authentication_link = grabber.get_authentication_link(email, password, year)
    with open("credentials.json", "w") as f:
        cred[
            "authentication_link_{}".format(parse_year_long(year))
        ] = authentication_link
        json.dump(cred, f)
    return authentication_link


def dfs_are_equal(df, other_df):
    return (
        df.shape == other_df.shape
        and ((df != other_df) & (df.notnull()) & (other_df.notnull())).any(axis=1).sum()
        == 0
    )


def save_df(df, filename, sheet_name):
    kwargs = {}
    if filename in os.listdir():
        kwargs["mode"] = "a"
        kwargs["if_sheet_exists"] = "replace"
    with pd.ExcelWriter(filename, engine="openpyxl", **kwargs) as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name, float_format="%.2f")


def scrape(
    year=YEAR,
    save=SAVE,
    skip_if_equal_to_last=SKIP_IF_EQUAL_TO_LAST,
    compute_min_pts=True,
    rank_save_path=None,
    min_pts_save_path=None,
    sheet_name=SHEET_NAME,
):
    if not "credentials.json" in os.listdir():
        print(
            "credentials.json not found in folder. This is necessary to read your email and password in order to sign-in in universitaly. Have you read README.md?"
        )
        raise FileNotFoundError

    authentication_link = load_credentials(year)
    workers = WORKERS or cpu_count()
    time_start = time.time()
    print(f"Starting with {workers} processes...", end="")
    try:
        rank_df = grabber.grab(
            year, authentication_link=authentication_link, workers=workers
        )
    except Exception as e:
        print("")
        raise e
    print(
        "Done in {:.2f} seconds, {} entries.".format(
            (time.time() - time_start),
            len(rank_df),
        )
    )
    min_pts_df = None
    if compute_min_pts:
        try:
            print("Computing min_pts...", end="")
            min_pts_df = (
                rank_df[rank_df["Specializzazione"].astype(bool)]
                .groupby(
                    ["Specializzazione", "Sede", "Contratto"],
                    as_index=False,
                )
                .aggregate({"#": max, "Tot": min})[
                    ["Specializzazione", "Sede", "Contratto", "#", "Tot"]
                ]
            )
            print("Done.")
        except Exception as e:
            print("")
            print("Error occurred while computing minimum points, skipping...")
            print(e)

    if save:
        print("Saving files:")
        rank_save_path = rank_save_path or "rank_{}.xlsx".format(parse_year_long(year))
        min_pts_save_path = min_pts_save_path or "min_pts_{}.xlsx".format(
            parse_year_long(year)
        )
        if skip_if_equal_to_last and os.path.exists(rank_save_path):
            sheets = sorted(pd.ExcelFile(rank_save_path).sheet_names, reverse=True)
            last_sheet_name = sheets[0]
            last_df = pd.read_excel(rank_save_path, sheet_name=last_sheet_name)
            equal = dfs_are_equal(rank_df, last_df)
            if not equal:
                save_df(rank_df, rank_save_path, sheet_name)
                print(f"Saved {rank_save_path}>{sheet_name}.")
            else:
                print(
                    f"Skipped saving rank as last_sheet ({last_sheet_name}) does not differ from now."
                )
        else:
            save_df(rank_df, rank_save_path, sheet_name)
            print(f"Saved {rank_save_path}>{sheet_name}.")
        if min_pts_df is not None:
            if skip_if_equal_to_last and os.path.exists(min_pts_save_path):
                sheets = sorted(
                    pd.ExcelFile(min_pts_save_path).sheet_names, reverse=True
                )
                last_sheet_name = sheets[0]
                last_df = pd.read_excel(min_pts_save_path, sheet_name=last_sheet_name)
                equal = dfs_are_equal(min_pts_df, last_df)
                if not equal:
                    save_df(min_pts_df, min_pts_save_path, sheet_name)
                    print(f"Saved {min_pts_save_path}>{sheet_name}.")
                else:
                    print(
                        f"Skipped saving min_pts as last_sheet ({last_sheet_name}) does not differ from now."
                    )
            else:
                save_df(min_pts_df, min_pts_save_path, sheet_name)
                print(f"Saved {min_pts_save_path}>{sheet_name}.")


def main():
    scrape(YEAR, compute_min_pts=COMPUTE_MIN_PTS)


if __name__ == "__main__":
    main()

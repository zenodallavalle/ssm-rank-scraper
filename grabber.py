from datetime import datetime as dt
from datetime import date as d
from multiprocessing import Pool, cpu_count
import numpy as np
from itertools import repeat
from requests import Session
from warnings import warn
import emoji
import re
from urllib.parse import urlparse, parse_qs
import pandas as pd
from bs4 import BeautifulSoup as BS

from year_parser import parse_year_long, parse_year_short, parse_year_int


BASE_URL = "https://www.universitaly.it/"


def get_columns(year):
    if parse_year_int(year) <= 2018:
        return ["#", "cognome_nome", "Tot", "Prova", "Titoli", "Stato", "Note"]
    else:
        return [
            "#",
            "cognome_nome",
            "Tot",
            "Prova",
            "Titoli",
            "Stato",
            "Note_sessione_straordinaria",
            "Note_immatricolazione",
        ]


class AuthenticationLinkNotFound(IndexError):
    """
    Error raised when authentication link cannot be found on the page in which is expected to be.
    This is likely due to wrong credentials or the fact that you did not signed up for ssm of that year or error on the cineca server.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(
            "Authentication link not found in page, are you sure email and password stored in credentials.json are correct? Were you signed up for ssm you are trying to download the rank? If this still comes up, try again later.",
            *args,
        )


def _initilize_find_authentication_link(year):
    long_year = parse_year_long(year)

    def find_authentication_link(tag):
        return (
            tag.name == "a"
            and "href" in tag.attrs
            and "ssm.cineca.it/autenticazione" in tag.attrs["href"]
            and f"year_ssm={long_year}" in tag.attrs["href"]
        )

    return find_authentication_link


# # THIS WAS FOR V1
# def get_authentication_link(email, password, year):
#     '''
#     This function get the authentication link to access the private page at ssm.cineca.it
#     Authentication link is strictly related to your account, don't share it.
#     '''
#     find_authentication_link = _initilize_find_authentication_link(year)
#     s = Session()
#     r = s.get('https://www.universitaly.it/index.php/login')
#     assert r.status_code == 200
#     bs = BS(r.content, 'lxml')
#     login_form = {}
#     form_tag = bs.find(lambda tag: tag.name == 'form' and 'login' in tag.attrs['id'])
#     form_url = form_tag.attrs['action']
#     for element in form_tag.find_all('input'):
#         if element.attrs['type'] != 'submit':
#             name = element.attrs['name']
#             if 'email' in name:
#                 value = email
#             elif 'password' in name:
#                 value = password
#             else:
#                 value = element.attrs['value']
#             login_form[name] = value
#     r = s.post(BASE_URL + form_url, login_form)
#     assert r.status_code == 200
#     r = s.get(BASE_URL + 'index.php/dashboard-ssm')
#     assert r.status_code == 200
#     bs = BS(r.content, 'lxml')
#     authentication_link = bs.find(find_authentication_link)
#     if not authentication_link:
#         raise AuthenticationLinkNotFound()
#     return authentication_link.attrs['href']

# # THIS WAS FOR V2
# def get_authentication_link(email, password, year):
#     """
#     This function get the authentication link to access the private page at ssm.cineca.it
#     Authentication link is strictly related to your account, don't share it.
#     """
#     s = Session()
#     r = s.get("https://www.universitaly.it/index.php/auth")
#     find_authentication_link = _initilize_find_authentication_link(year)
#     bs = BS(r.content, "lxml")
#     url_form_submit = bs.find("form", {"id": "kc-form-login"}).attrs["action"]

#     payload = {"username": email, "password": password, "credentialId": None}

#     r = s.post(url_form_submit, data=payload)
#     s.get("https://www.universitaly.it/index.php/auth")
#     r = s.get("https://www.universitaly.it/index.php/dashboard-ssm")
#     bs = BS(r.content, "lxml")
#     authentication_link = bs.find(find_authentication_link)
#     return authentication_link.attrs["href"]


# THIS IS THE ACTUAL V3
def get_authentication_link(email, password, year):
    """
    This function get the authentication link to access the private page at ssm.cineca.it
    Authentication link is strictly related to your account, don't share it.
    """
    s = Session()
    r = s.get("https://universitaly-private.cineca.it/index.php/auth")
    find_authentication_link = _initilize_find_authentication_link(year)
    bs = BS(r.content, "lxml")
    url_form_submit = bs.find("form", {"id": "kc-form-login"}).attrs["action"]

    payload = {"username": email, "password": password, "credentialId": None}

    r = s.post(url_form_submit, data=payload)
    if "Ciao" not in r.text:
        raise ValueError("Wrong credentials")
    r = s.get("https://universitaly-private.cineca.it/index.php/dashboard-ssm")
    bs = BS(r.content, "lxml")
    authentication_link = bs.find(find_authentication_link)
    return authentication_link.attrs["href"]


def _extract_previdence_code_from_authentication_link(authentication_link):
    parsed_url = urlparse(authentication_link)
    parsed_qs = parse_qs(parsed_url.query)
    user = parsed_qs["username"][0]
    user = user.replace("MEM", "")
    user = re.sub("_[0-9]{2,4}.*$", "", user).strip()
    return user


def authenticate(authentication_link=None, session=None):
    """
    Authenticate the session to ssm.cineca.it in order to access the ranking.
    """
    s = session or Session()
    r = s.get(authentication_link)
    assert r.status_code == 200
    return s


def gen_url_paged(year, n, previdence_code):
    """
    Takes ranking year and number of page and return a string of the url you need to get to access that data.
    """
    short_year = parse_year_short(year)
    long_year = parse_year_long(year)
    BASE = f"https://ssm.cineca.it/ssm{short_year}_graduatoria.php?user=MEM{previdence_code}_{short_year}&year_ssm={long_year}"
    return BASE + f"&page={n}"


def _convert_option_text_to_integer(tag):
    return int(tag.text.strip())


def detect_limit(s, year, previdence_code):
    """
    Takes request.Session instance authenticated as first argument and year and returns how many pages the ranking has.
    """
    r = s.get(gen_url_paged(year, 1, previdence_code))
    assert r.status_code == 200
    bs = BS(r.content, "lxml")
    select = bs.find("select", {"id": "selPag"})
    return max(map(_convert_option_text_to_integer, select.find_all("option")))


def parse_data(tds, year):
    """
    Parse data for every row in the page.
    Return a dictionary containing columns as keys and values scraped from the page.
    """
    columns = get_columns(year=year)
    row = {}
    for i, c in enumerate(columns):
        if i < 5:
            try:
                row[c] = float(tds[i].text.replace(",", "."))
            except ValueError:
                row[c] = (
                    tds[i].text.strip() if i != 1 else list(tds[i].children)[2].strip()
                )
        elif i == 5:
            span = tds[i].find("span")
            value = span.attrs.get("title", None)
            if not value:
                value = span.text.strip()
            row[c] = value
        elif i >= 6:
            if len(tds) < 7:
                row[c] = ""
                continue
            span = tds[i].find("span")
            if span is not None:
                children = list(span.children)
                text = children[0].strip()
                row[c] = text
                type_of_c = c.split("_")[0]
                if len(children) == 1:
                    row[f"Contratto_{type_of_c}"] = "STAT"
                else:
                    contract = children[1].text.strip().upper()
                    if (
                        not contract
                        or len(contract) == 1
                        or emoji.emoji_count(contract)
                    ):
                        contract = children[1].attrs.get("title", None)
                        if contract:
                            contract = contract.strip()
                    row[f"Contratto_{type_of_c}"] = contract

            else:
                text = tds[i].text.strip()
                row[c] = text
        else:
            row[c] = tds[i].text.strip()
    return row


def parse_birthday(x):
    """
    Parse "cognome_nome" column in order to extract a datetime.date representation of the birthdate. If an error occurs None is returned
    """
    try:
        string_date = x.rsplit("(", 1)[1].replace(")", "").strip()
        datetime = dt.strptime(string_date, "%d/%m/%Y")
        return d(datetime.year, datetime.month, datetime.day)
    except Exception:
        return


def scan_page(
    session,
    year,
    previdence_code,
    n,
    request_status_callback=lambda r: r.status_code,
    empty_page_callback=lambda: None,
):
    """
    Parse the page number (n)
    request_status_callback (optional) function called when request's status_code != 200; passed args = [request]
    empty_page_callback (optional) function called when there are no elements to analyze in page; passed args = []
    If the page is parsed correctly a pd.DataFrame instance containing the elements in the page is returned.
    """
    r = session.get(gen_url_paged(year, n, previdence_code))
    if r.status_code != 200:
        return request_status_callback(r)
    bs = BS(r.content, "lxml")
    trs = bs.find_all("tr")
    rows = []
    try:
        if len(trs) < 2:
            return empty_page_callback()
        for i, tr in enumerate(trs):
            tds = tr.findChildren("td")
            if len(tds) > 0:
                rows.append(parse_data(tds, year))
    except Exception as e:
        print(f"Error while parsing page {n}, tr: {i}, {e}")
        raise e
    return pd.DataFrame(rows)


def grab(year, email=None, password=None, authentication_link=None, workers=None):
    # Number of _workers is equal to passed argument workers or if None is equal to cpu_count()
    workers = workers or cpu_count()
    # Get a session authenticated to access the ranking
    if authentication_link is None:
        warn("Providing authentication link can make the process ~5s slower!")
        authentication_link = get_authentication_link(email, password, year)
    previdence_code = _extract_previdence_code_from_authentication_link(
        authentication_link
    )
    s = authenticate(authentication_link)
    # detect limit
    upper_limit = detect_limit(s, year, previdence_code)

    with Pool(workers) as p:
        dfs = p.starmap(
            scan_page,
            zip(
                repeat(s),
                repeat(year),
                repeat(previdence_code),
                range(1, upper_limit + 1),
            ),
        )
        df = pd.concat([df for df in dfs if isinstance(df, pd.DataFrame)])

    # generate column birth from date within parenthesis
    df["Nascita"] = df["cognome_nome"].map(parse_birthday)
    df["CognomeNome"] = df["cognome_nome"].map(lambda x: x.rsplit("(", 1)[0].strip())

    exceptions = False
    if df["#"].str.contains("*", regex=False).any():
        exceptions = True
        # It has some exceptions, so add a column to mark them and keep the number
        df["Exception"] = df["#"].str.contains("*", regex=False).replace({False: None})
        df["#"] = (
            df["#"].replace({"(*)": np.nan}).fillna(df["#"].shift(1)).add(1).astype(int)
        )
    else:
        df["#"] = df["#"].astype(int)
    # Sort by #
    df = df.sort_values(by=["#"])

    df = df.reset_index(drop=True)

    if parse_year_int(year) <= 2018:
        df["Note"] = df["Note"].astype(str)
        df[["Specializzazione", "Sede"]] = (
            df["Note"].astype(str).str.rsplit(",", n=1, expand=True)
        )
        df["Specializzazione"] = df["Specializzazione"].str.strip()
        df["Sede"] = df["Sede"].str.strip()
        cols = [
            "#",
            "CognomeNome",
            "Nascita",
            "Tot",
            "Prova",
            "Titoli",
            "Stato",
            "Contratto",
            "Specializzazione",
            "Sede",
            "Note",
        ]
    else:
        cols = [
            "#",
            "CognomeNome",
            "Nascita",
            "Tot",
            "Prova",
            "Titoli",
            "Stato",
            "Contratto_sessione_straordinaria",
            "Specializzazione_sessione_straordinaria",
            "Sede_sessione_straordinaria",
            "Contratto_sessione_immatricolazione",
            "Specializzazione_immatricolazione",
            "Sede_immatricolazione",
            "Note_sessione_straordinaria",
            "Note_immatricolazione",
        ]
        df[
            ["Specializzazione_sessione_straordinaria", "Sede_sessione_straordinaria"]
        ] = (
            df["Note_sessione_straordinaria"]
            .astype(str)
            .str.rsplit(",", n=1, expand=True)
        )
        df["Specializzazione_sessione_straordinaria"] = df[
            "Specializzazione_sessione_straordinaria"
        ].str.strip()
        df["Sede_sessione_straordinaria"] = df[
            "Sede_sessione_straordinaria"
        ].str.strip()

        df[["Specializzazione_immatricolazione", "Sede_immatricolazione"]] = (
            df["Note_immatricolazione"]
            .astype(str)
            .str.replace(
                "Medicina d'emergenza-urgenza", "Medicina d'emergenza$urgenza"
            )  # Replace - in specializations with $ to avoid splitting, remember to add new specializations with - here
            .str.replace("Chirurgia maxillo-facciale", "Chirurgia maxillo$facciale")
            .str.split("-", n=1, expand=True)
        )
        df["Specializzazione_immatricolazione"] = (
            df["Specializzazione_immatricolazione"].str.replace("$", "-").str.strip()
        )
        df["Sede_immatricolazione"] = df["Sede_immatricolazione"].str.strip()
    if exceptions:
        cols.insert(1, "Exception")
    for col in cols:
        if col not in df.columns:
            df[col] = np.nan

    df = df[cols]

    # rename index col "index"
    df = df.rename_axis(["index"], axis=1)
    df = df.replace([np.nan, ""], [None, None])

    return df


def _get_number_of_contract_url(year, previdence_code):
    short_year = parse_year_short(year)
    long_year = parse_year_long(year)
    return f"https://ssm.cineca.it/ssm{short_year}_riepilogo.php?user=MEM{previdence_code}_{short_year}&year_ssm={long_year}"


def download_number_of_contracts(
    year, email=None, password=None, authentication_link=None
):
    # Get a session authenticated to access the ranking
    if authentication_link is None:
        warn("Providing authentication link can make the process ~5s slower!")
        authentication_link = get_authentication_link(email, password, year)
    previdence_code = _extract_previdence_code_from_authentication_link(
        authentication_link
    )
    s = authenticate(authentication_link)
    r = s.get(_get_number_of_contract_url(year=year, previdence_code=previdence_code))
    assert r.status_code == 200

    # Parse the page
    bs = BS(r.content, "lxml")
    data = {}
    columns_dict = {}

    table = bs.find("table")
    if table is None:
        raise ValueError("Table not found in page")
    trs = table.find_all("tr")
    if trs:
        for tr in trs:
            ths = tr.find_all("th")
            tds = tr.find_all("td")
            _columns = ths or tds
            if not _columns:
                continue
            if not data:
                data = dict([(i, []) for i in range(len(_columns))])
            if ths:
                for i, th in enumerate(ths):
                    columns_dict[i] = th.text.strip().title()
            if tds:
                for i, td in enumerate(tds):
                    if i == len(tds) - 1:
                        data[i].append(int(td.text.strip()))
                    else:
                        data[i].append(td.text.strip())

    df = pd.DataFrame(data)
    df = df.rename(columns=columns_dict)
    df = df.replace([""], np.nan)
    return df

from datetime import datetime as dt
from datetime import date as d
from multiprocessing import Pool, cpu_count
from itertools import repeat
from requests import Session

from warnings import warn

import pandas as pd
from bs4 import BeautifulSoup as BS

from year_parser import parse_year_long, parse_year_short


BASE_URL = 'https://www.universitaly.it/'

COLUMNS = ['#', 'cognome_nome', 'Tot', 'Prova', 'Titoli', 'Stato', 'Note']


class AuthenticationLinkNotFound(IndexError):
    """
    Error raised when authentication link cannot be found on the page in which is expected to be.
    This is likely due to wrong credentials or the fact that you did not signed up for ssm of that year or error on the cineca server.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(
            'Authentication link not found in page, are you sure email and password stored in credentials.json are correct? Were you signed up for ssm you are trying to download the rank? If this still comes up, try again later.',
            *args,
        )


def _initilize_find_authentication_link(year):
    long_year = parse_year_long(year)

    def find_authentication_link(tag):
        return (
            tag.name == 'a'
            and 'href' in tag.attrs
            and 'ssm.cineca.it/autenticazione' in tag.attrs['href']
            and f'year_ssm={long_year}' in tag.attrs['href']
        )

    return find_authentication_link


def get_authentication_link(email, password, year):
    '''
    This function get the authentication link to access the private page at ssm.cineca.it
    Authentication link is strictly related to your account, don't share it.
    '''
    find_authentication_link = _initilize_find_authentication_link(year)
    s = Session()
    r = s.get('https://www.universitaly.it/index.php/login')
    assert r.status_code == 200
    bs = BS(r.content, 'lxml')
    login_form = {}
    form_tag = bs.find(lambda tag: tag.name == 'form' and 'login' in tag.attrs['id'])
    form_url = form_tag.attrs['action']
    for element in form_tag.find_all('input'):
        if element.attrs['type'] != 'submit':
            name = element.attrs['name']
            if 'email' in name:
                value = email
            elif 'password' in name:
                value = password
            else:
                value = element.attrs['value']
            login_form[name] = value
    r = s.post(BASE_URL + form_url, login_form)
    assert r.status_code == 200
    r = s.get(BASE_URL + 'index.php/dashboard-ssm')
    assert r.status_code == 200
    bs = BS(r.content, 'lxml')
    authentication_link = bs.find(find_authentication_link)
    if not authentication_link:
        raise AuthenticationLinkNotFound()
    return authentication_link.attrs['href']


def authenticate(email, password, year, authentication_link=None, session=None):
    '''
    Authenticate the session to ssm.cineca.it in order to access the ranking.
    If authentication link is provided it's ~5s faster.
    '''
    auth_link = authentication_link
    if auth_link is None:
        auth_link = get_authentication_link(email, password, year)
        warn(
            'Using authentication_link next time can save ~5 seconds. You can get it with get_authentication_link function.'
        )
    s = session or Session()
    r = s.get(auth_link)
    assert r.status_code == 200
    return s


def gen_url_paged(year, n):
    """
    Takes ranking year and number of page and return a string of the url you need to get to access that data.
    """
    short_year = parse_year_short(year)
    long_year = parse_year_long(year)
    BASE = f'https://ssm.cineca.it/ssm{short_year}_graduatoria.php?user=MEMDLLZNE96D25L781A_{short_year}&year_ssm={long_year}'
    return BASE + f'&page={n}'


def _convert_option_text_to_integer(tag):
    return int(tag.text.strip())


def detect_limit(s, year):
    """
    Takes request.Session instance authenticated as first argument and year and returns how many pages the ranking has.
    """
    r = s.get(gen_url_paged(year, 1))
    assert r.status_code == 200
    bs = BS(r.content, 'lxml')
    select = bs.find('select', {'id': 'selPag'})
    return max(map(_convert_option_text_to_integer, select.find_all('option')))


def prepare_data(tds):
    '''
    Parse data for every row in the page.
    Return a dictionary containing columns as keys and values scraped from the page.
    '''
    row = {}
    for i, c in enumerate(COLUMNS):
        if i < 5:
            try:
                row[c] = float(tds[i].text.replace(',', '.'))
            except ValueError:
                row[c] = (
                    tds[i].text.strip() if i != 1 else list(tds[i].children)[2].strip()
                )
        elif i == 5:
            span = tds[i].find('span')
            value = span.attrs.get('title', None)
            if not value:
                value = span.text.strip()
            row[c] = value
        elif i == 6:
            span = tds[i].find('span')
            if span:
                children = list(span.children)
                row[c] = children[0].strip()
                row['Contratto'] = children[1].text.strip().upper()
            else:
                row[c] = tds[i].text.strip()
        else:
            row[c] = tds[i].text.strip()
    return row


def parse_birthday(x):
    """
    Parse "cognome_nome" column in order to extract a datetime.date representation of the birthdate. If an error occurs None is returned
    """
    try:
        string_date = x.rsplit('(', 1)[1].replace(')', '').strip()
        datetime = dt.strptime(string_date, '%d/%m/%Y')
        return d(datetime.year, datetime.month, datetime.day)
    except Exception:
        return


def parse_specializzazione_sede(df):
    """
    Parse df['Note'] in order to extract specializzazione - sede combination (a tuple). If error occurs None is returned.
    """
    x = df['Note']
    try:
        specializzazione, sede = x.rsplit(',', 1)
        return specializzazione.strip(), sede.strip()
    except Exception:
        return None, None


def scan_page(
    session,
    year,
    n,
    request_status_callback=lambda r: r.status_code,
    empty_page_callback=lambda: None,
):
    '''
    Parse the page number (n)
    request_status_callback (optional) function called when request's status_code != 200; passed args = [request]
    empty_page_callback (optional) function called when there are no elements to analyze in page; passed args = []
    If the page is parsed correctly a pd.DataFrame instance containing the elements in the page is returned.
    '''
    r = session.get(gen_url_paged(year, n))
    if r.status_code != 200:
        return request_status_callback(r)
    bs = BS(r.content, 'lxml')
    trs = bs.find_all('tr')
    rows = []
    if len(trs) < 2:
        return empty_page_callback()
    for tr in trs:
        tds = tr.findChildren('td')
        if len(tds) > 0:
            rows.append(prepare_data(tds))
    return pd.DataFrame(rows)


def grab(year, email=None, password=None, authentication_link=None, workers=None):
    # Number of _workers is equal to passed argument workers or if None is equal to cpu_count()
    workers = workers or cpu_count()
    # Get a session authenticated to access the ranking
    s = authenticate(
        email,
        password,
        year,
        authentication_link,
    )
    # detect limit
    upper_limit = detect_limit(s, year)

    with Pool(workers) as p:
        dfs = p.starmap(
            scan_page, zip(repeat(s), repeat(year), range(1, upper_limit + 1))
        )
        df = pd.concat([df for df in dfs if isinstance(df, pd.DataFrame)])

    # generate column birth from date within parenthesis
    df['Nascita'] = df['cognome_nome'].map(parse_birthday)
    df['CognomeNome'] = df['cognome_nome'].map(lambda x: x.rsplit('(', 1)[0].strip())
    df['Note'] = df['Note'].astype(str)

    df['#'] = df['#'].astype(int)
    df.sort_values(by=['#'], inplace=True)

    df.reset_index(drop=True, inplace=True)

    df[['Specializzazione', 'Sede']] = df.apply(
        parse_specializzazione_sede, axis=1, result_type='expand'
    )

    cols = [
        '#',
        'CognomeNome',
        'Nascita',
        'Tot',
        'Prova',
        'Titoli',
        'Stato',
        'Contratto',
        'Specializzazione',
        'Sede',
        'Note',
    ]

    df = df[cols]

    # rename index col "index"
    df.rename_axis(['index'], axis=1, inplace=True)
    return df

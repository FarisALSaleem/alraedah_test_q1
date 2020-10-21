from bs4 import BeautifulSoup
from requests import get
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_setup import Category, Company, Base
import pandas as pd


def get_html(url):
    req = get(url)
    if req.status_code is 200:
        return req.content
    print("Error: {}".format(req.status_code))
    return None


def get_data(soup):
    # table 13 is the id of html element that parents all the data we want to scrape
    # selected tbody then final all tr element
    # each tr element represents one company
    # loop through tr element creating a company dict and add it to a list
    rows = soup.find(id="table13").find('tbody').find_all('tr')
    df = list()
    for row in rows:
        attribute = row.find_all('td')
        company = dict(category=attribute[0].string,
                       name=attribute[1].a.string,
                       last_trade_price=attribute[2].string,
                       last_trade_volume=attribute[3].string,
                       last_trade_change_value=attribute[4].string,
                       last_trade_percentage_change=attribute[5].string,
                       cumulative_number_of_trades=attribute[6].string,
                       cumulative_volume_traded=attribute[7].string,
                       best_bid_price=attribute[8].string,
                       best_bid_volume=attribute[9].string)
        df.append(company)
    # return list of companies
    return df


def get_categories(df):
    # loop through the companies list and add each company category to a set
    # the reason why its done like this is because the category 'Equity Rights' is not a available drop-down list
    categories = set()
    for row in df:
        categories.add(row['category'])
    return categories


def clean_data(df, fields):
    for row in df:
        for field in fields:
            # remove , from numbers
            # example 2,000 => 2000
            row[field] = row[field].replace(",", "")
            # remove - from a field that hasn't changed in value
            row[field] = row[field].replace("-", "0")
    return df


def connect_to_db(db_url):
    engine = create_engine(db_url)
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    return DBSession()


def push_category_to_db(session, categories):
    for category in categories:
        session.add(Category(name=category))
        session.commit()


def push_companies_to_db(session, df):
    for company in df:
        category_id = session.query(Category).filter_by(name=company['category']).one().id
        session.add(Company(name=company['name'],
                            last_trade_price=company['last_trade_price'],
                            last_trade_volume=company['last_trade_volume'],
                            last_trade_change_value=company['last_trade_change_value'],
                            last_trade_percentage_change=company['last_trade_percentage_change'],
                            cumulative_number_of_trades=company['cumulative_number_of_trades'],
                            cumulative_volume_traded=company['cumulative_volume_traded'],
                            best_bid_price=company['best_bid_price'],
                            best_bid_volume=company['best_bid_volume'],
                            category_id=category_id))
        session.commit()


def query_all_categories(session):
    query = session.query(Category).all()
    df = list()
    for category in query:
        df.append(category.serialize)
    return pd.DataFrame.from_records(df)


def query_all_companies(session):
    query = session.query(Company).all()
    df = list()
    for category in query:
        df.append(category.serialize)
    return pd.DataFrame.from_records(df)


def main():
    # get html file
    url = 'https://www.tadawul.com.sa/wps/portal/tadawul/markets/equities/market-watch/market-watch-today?locale=en'
    html = get_html(url)

    # exit if file fails to download
    if html is None:
        exit(1)

    soup = BeautifulSoup(html, "html.parser")

    # scrape data
    df = get_data(soup)

    # clean data
    fields_to_be_cleaned = ['last_trade_price', 'last_trade_volume',
                            'last_trade_change_value', 'last_trade_percentage_change',
                            'cumulative_number_of_trades', 'cumulative_volume_traded',
                            'best_bid_price', 'best_bid_volume']
    df = clean_data(df, fields_to_be_cleaned)

    # get_categories
    categories = get_categories(df)

    # connect_to_db
    db_url = 'postgresql://alraedah_user:plain_text@localhost/tadwul'
    session = connect_to_db(db_url)

    # push scrape data to db
    push_category_to_db(session, categories)
    push_companies_to_db(session, df)
    print("all the categories and companies have been added to the database")

    # query db
    df_category = query_all_categories(session)
    df_companies = query_all_companies(session)

    # show tables
    print(df_category)
    print(df_companies.head(10))

    # write csv
    df_category.to_csv('category_table.csv')
    df_companies.to_csv('company_table.csv')
    print("csv written")


if __name__ == '__main__':
    main()

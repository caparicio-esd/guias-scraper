
from bs4 import BeautifulSoup
from pandas import DataFrame, Series
import requests
from fields import queryable_dict

home_url: str = "https://guias.esdmadrid.es"
home_html: str = ""
home_soup: BeautifulSoup = None
dataset: DataFrame = None


def get_home_page() -> None:
    global home_html, home_soup, dataset

    # get data
    r = requests.get(home_url)
    # parse data
    home_html = r.text
    home_soup = BeautifulSoup(home_html, features="html.parser")
    home_titles = home_soup\
        .select(".view-listado-de-guias-docentes.view-display-id-page_1")[0]\
        .select("[headers=view-title-table-column] a")

    # process data
    titles_series = Series(title.string for title in home_titles)
    titles_hrefs = Series(title.get("href") for title in home_titles)
    titles_hrefs = home_url + titles_hrefs
    dataset = DataFrame(data={'title': titles_series, 'href': titles_hrefs})

    # save transactional data
    dataset.to_csv("./data/transactional_dbs/home_data.csv")


def get_single_page(id: int) -> None:
    global dataset
    row = dataset.iloc[id]
    r = requests.get(row["href"])
    r_soup = BeautifulSoup(r.text, features="html.parser")

    for field in queryable_dict:
        if (field["item"]):
            dataset.at[id, field["name"]] = r_soup.select_one(
                "[class*={}] .{}".format(field[
                    "name"], field["item"])).get_text()
            # TODO:<----------------------------- transformers

    print(dataset.iloc[id])
    dataset.to_csv("./data/transactional_dbs/home_dataa.csv")


if __name__ == "__main__":
    get_home_page()
    for i in range(0, dataset.size):
        get_single_page(i)

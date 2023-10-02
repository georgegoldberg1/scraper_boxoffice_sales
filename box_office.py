# %%
import pandas as pd
import bs4
import requests
import os

import seaborn as sns
import plotly.express as px
import plotly.io as pio

# pio.templates


class SalesRetriever:
    """_summary_"""

    def __init__(self):
        self._movie_ids = {  # add movies here with their correct id
            "Barbie": "1077904129",
            "Oppenheimer": "3725886209",
        }
        self.movies = list(self._movie_ids)
        self._data_cache = dict()
        self._local_storage = "_data_cache/titles/"
        if not os.path.exists(self._local_storage):
            os.makedirs(self._local_storage)

    def _get_title_url(self, title: str):
        title_id = self._movie_ids.get(title)
        prefix_url = "https://www.boxofficemojo.com/release/rl"
        suffix_url = "/?ref_=bo_tt_gr_1"
        url = "".join([prefix_url, title_id, suffix_url])
        return url

    def _get_local_filepath(self, title: str):
        """Return appropriate filename and folderpath for each Movie.

        Movies are saved by ID; this function helps locate them by name.

        Args:
            title (str): The name of the movie

        Returns:
            str: example output "localfolder/title_00000000001.html"
        """
        title_id = self._movie_ids.get(title)
        local_file = "".join(["title_", title_id, ".html"])
        local_folder = self._local_storage
        filepath = "".join([local_folder, local_file])
        return filepath

    def _save_local_file(self, html: str, title: str):
        """Save html to file
        filename is the movie ID (after release/rl in the url string)

        Args:
            html (str): full string of html can be loaded in BeautifulSoup
            title (str): Movie
        """
        filepath = self._get_local_filepath(title)
        with open(filepath, "w") as f:
            f.write(html)
        print(f"Local file saved: ({filepath})")
        return

    def _load_local_file(self, title: str):
        """Load html from file
        filename is the movie ID (after release/rl in the url string)

        Args:
            title (str): Movie title
        """
        filepath = self._get_local_filepath(title)
        with open(filepath, "r") as f:
            html = f.read()
        print(f"Local file loaded: ({filepath})")
        return html

    def _make_web_request(self, url: str):
        print(f"Requesting webpage ({url})")

        # Request site content
        response = requests.get(url)
        status = response.status_code

        if status == 200:
            # print(f"Successful Response (Status Code {status})")
            html = response.text
            return html
        elif status in range(100, 200):
            error_string = f"Informational Response (Status Code {status})"
            raise RuntimeError(error_string)
        elif status in range(300, 400):
            error_string = f"Redirect (Status Code {status})"
            raise RuntimeError(error_string)
        elif status in range(400, 500):
            error_string = f"Client Error (Status Code {status})"
            raise RuntimeError(error_string)
        elif status in range(500, 600):
            error_string = f"Server Error (Status Code {status})"
            raise RuntimeError(error_string)
        else:
            print("response code unknown")
            html = response.text
            # If html_list is a list of strings, join them into a single string.
            if isinstance(html, list):
                html = "\n".join(html)
            return html

    def _parse_soup(self, html):
        try:
            soup = bs4.BeautifulSoup(html, "lxml")
        except:
            soup = bs4.BeautifulSoup(html, "html.parser")
        return soup

    def _update_movie_index(self, extraction_folder: str):
        dfs = []

        for year_file in os.listdir(extraction_folder):
            if ".csv" in year_file:
                year = year_file.replace("movies_", "").replace(".csv", "")
                tmp = pd.read_csv("/".join([extraction_folder, year_file]))
                tmp["year"] = year
                # # add year to the dates
                tmp["Release Date"] = tmp["Release Date"] + f" {year}"
                dfs.append(tmp)

        _df = pd.concat(dfs)

        fmt_currency = lambda x: int(str(x).replace("$", "").replace(",", ""))
        _df["Total Gross"] = _df["Total Gross"].map(fmt_currency)
        _df["Gross"] = _df["Gross"].map(fmt_currency)

        # Export
        _df = _df.sort_values(["year", "Gross"], ascending=False)
        export_path = "/".join([extraction_folder, "historical_releases.csv"])
        _df.to_csv(export_path, index=False)

        # Update the index
        for_update = (
            _df[~_df["movie_id"].isna()][["Release", "movie_id"]]
            .drop_duplicates()
            .set_index("Release")["movie_id"]
            .astype(int)
            .astype(str)
        ).to_dict()

        self._movie_ids.update(for_update)
        self.movies = list(self._movie_ids)

    def load_more_movies(self):
        print(f"Pulling Movie titles for year:", end=" ")
        for year in range(2023, 1990 + 1, -1):
            print(year, end=",")
            extraction_folder = self._local_storage.split("/")[0]
            filepath = "/".join([extraction_folder, f"movies_{year}.csv"])

            # Skip this year if the file is already downloaded
            if os.path.exists(filepath):
                continue

            url_string = f"https://www.boxofficemojo.com/year/{year}/?ref_=bo_hm_yrdom"

            html = self._make_web_request(url_string)
            # soup = bs4.BeautifulSoup(html)
            soup = self._parse_soup(html)
            all_movie_metadata = pd.read_html(url_string)[0]

            movies = [
                movie.find("a")
                for movie in soup.find_all(
                    "td",
                    {"class": "a-text-left mojo-field-type-release mojo-cell-wide"},
                )
            ]
            movie_ids = {
                movie.get_text(): movie["href"].replace("/release/rl", "").split("/")[0]
                for movie in movies
            }

            all_movie_metadata["movie_id"] = all_movie_metadata["Release"].map(
                movie_ids
            )
            all_movie_metadata.to_csv(filepath, index=False)
        print(f"Data has been extracted to: {extraction_folder}")

        # Update the index
        self._update_movie_index(extraction_folder)
        print('Index Updated successfully, see ".movies"')

    def _html_to_dataframe(self, html):
        # Extract daily sales table
        # try:
        #     soup = bs4.BeautifulSoup(html, "lxml")
        # except:
        #     soup = bs4.BeautifulSoup(html, "html.parser")
        soup = self._parse_soup(html)

        data = [
            [row.find("a")["href"].replace("/date/", "").split("/")[0]]
            + [cell.get_text() for cell in row.find_all("td")[1:]]
            for row in soup.find_all("tr")[1:]
        ]
        cols = [head.get_text().strip() for head in soup.find_all("th")]

        # Generate DataFrame
        _df = pd.DataFrame(data, columns=cols).dropna(how="all")

        # fmt dates
        _df["Date"] = pd.to_datetime(_df["Date"], yearfirst=True)

        # fmt currency
        fmt_currency = lambda x: int(x.replace("$", "").replace(",", ""))
        _df["Daily"] = _df["Daily"].map(fmt_currency)
        _df["Avg"] = _df["Avg"].map(fmt_currency)
        _df["To Date"] = _df["To Date"].map(fmt_currency)

        # fmt numeric
        fmt_num = lambda x: int(x.replace("$", "").replace(",", ""))
        _df["Theaters"] = _df["Theaters"].map(fmt_num)
        _df["Day"] = _df["Day"].map(fmt_num)

        return _df

    def daily_sales(self, title: str, full_refresh: bool = False):
        """Return daily sales data.

        Look for local files first before hitting the site.

        Args:
            title (str): The movie you want data for. use SalesRetriever.movies to see the available titles
            full_refresh (bool, optional): will load local data rather than repulling from site each time. Defaults to False.
        """
        filepath = self._get_local_filepath(title)

        # Request the webpage if no file exists locally, or if forced
        if (not os.path.exists(filepath)) or full_refresh:
            html = self._make_web_request(url=self._get_title_url(title))
            self._save_local_file(html, title)
            _df = self._html_to_dataframe(html)
            self._data_cache[title] = _df
            return _df

        # Attempt to load from memory
        elif title in self._data_cache:
            _df = self._data_cache.get(title)
            return _df

        # Attempt to load from local storage
        else:
            html = self._load_local_file(title)
            _df = self._html_to_dataframe(html)
            self._data_cache[title] = _df
            return _df


# %%
boxoffice = SalesRetriever()
opp = boxoffice.daily_sales("Oppenheimer")
barb = boxoffice.daily_sales("Barbie")

# %%
boxoffice.load_more_movies()
# %%
boxoffice.movies

# %%

# 'Barbie',
#  'Oppenheimer',
#  'Avengers: Endgame',
#  'Top Gun: Maverick',
#  'Black Panther',
#  'Avengers: Infinity War',
#  'Jurassic World',
#  'Star Wars: Episode VII - The Force Awakens',
#  'The Avengers',
#  'Incredibles 2',
#  'The Super Mario Bros. Movie',
#  'Spider-Man: No Way Home',
#  'The Lion King',
#  'The Dark Knight',
#  'Star Wars: Episode VIII - The Last Jedi',
#  'Beauty and the Beast',

movies2023 = pd.read_csv("_data_cache/movies_2023.csv")

# %%
boxoffice.daily_sales("Barbie")

# %%

boxoffice._data_cache
tot = len(boxoffice.movies)
for idx, movie in enumerate(boxoffice.movies):
    if idx / tot < 0.25:
        pass
    elif idx / tot < 0.50:
        print("25% done")
    elif idx / tot < 0.75:
        print("50% done")
    else:
        print("75% done")

    print(f"Movie: {movie}", end=" ")

    try:
        boxoffice.daily_sales(movie)
    except:
        print(f"\n---failed: {movie}")


# %%
len(boxoffice.movies)
# %%
boxoffice
# %%
dfs = []

for movie, df in boxoffice._data_cache.items():
    dfs.append(df.assign(movie_title=movie))

df = pd.concat(dfs)
# %%


# %%

sns.kdeplot(df.query("movie_title == 'Oppenheimer'")["Daily"], color="black")
sns.kdeplot(df.query("movie_title == 'Barbie'")["Daily"], color="pink")
# %%
len(df.query("movie_title == 'Barbie'")["Daily"]), len(
    df.query("movie_title == 'Oppenheimer'")["Daily"]
),
# %%


fig_dow = px.violin(
    d, x="DOW", y="Daily", facet_row="movie_title", title="By Day of Week"
)
# fig_dow.update_yaxes(lambda x:x.update(x.text=x.))
fig_dow

# %%
df.columns
# %%
d = df.query("movie_title in ['Oppenheimer', 'Barbie']")

fig1 = px.bar(
    d.groupby("movie_title")["Daily"].sum().reset_index(),
    x="movie_title",
    y="Daily",
    color="movie_title",
    color_discrete_map={"Oppenheimer": "red", "Barbie": "#E0218A"},
    template="plotly_dark",
)
fig1.update_layout(title="Total US Gross Sales (USD)")
fig1
# %%
fig2 = px.line(
    d,
    x="Date",
    y="Daily",
    color="movie_title",
    color_discrete_map={"Oppenheimer": "red", "Barbie": "#E0218A"},
    template="plotly_dark",
)
fig2.update_layout(yaxis={"title": "Daily US Gross Sales (USD)"})
fig2
# %%
# Prepare data
df_2023 = df[df.Date.dt.year == 2023]
top7_titles = (
    df_2023.groupby("movie_title")["Daily"]
    .sum()
    .sort_values(ascending=False)
    .iloc[:7]
    .index
)
top7_titles = df_2023[df_2023.movie_title.isin(top7_titles)]
other_titles = (
    df_2023.groupby("movie_title")["Daily"]
    .sum()
    .sort_values(ascending=False)
    .iloc[7:]
    .index
)
other_titles = (
    df_2023[df_2023.movie_title.isin(other_titles)]
    .groupby("Date")["Daily"]
    .sum()
    .reset_index()
    .assign(movie_title="Other")
)
d = pd.concat(
    [
        top7_titles[["Date", "Daily", "movie_title"]],
        other_titles[["Date", "Daily", "movie_title"]],
    ]
)
# plot_order = ["Other"] + list(
#     top7_titles.groupby("movie_title")["Daily"].sum().sort_values().index
# )
# plot_order = {title: order for order, title in enumerate(plot_order)}
# d.sort_values(by="movie_title", key=plot_order)  # key=lambda x:plot_order.get(x))

# Plot top titles + area chart for rest.
fig3 = px.area(
    data_frame=d,
    x="Date",
    y="Daily",
    color="movie_title",
    color_discrete_map={"Oppenheimer": "red", "Barbie": "#E0218A"},
    # color_discrete_sequence=plot_order
)
# remove outer edge line of area color
fig3.for_each_trace(lambda trace: trace.update(fillcolor=trace.line.color))
fig3.update_layout(
    yaxis={"title": "US Gross Sales (USD)"},
    width=800,
    template="plotly_dark",
)
fig3
# %%
with open("plotly Barbie v Oppenheimer.html", "w") as f:
    # bar chart (Barbie v Oppen)
    f.write(fig1.to_html(full_html=False, include_plotlyjs="cdn"))
    # line chart (Barbie v Oppen)
    f.write(fig2.to_html(full_html=False, include_plotlyjs="cdn"))
    # area plot
    f.write(fig3.to_html(full_html=False, include_plotlyjs="cdn"))
# %%
# px.treemap(d.groupby("movie_title")["To Date"], values='To Date')

# %%


# %%
plot_order = ["Other"] + list(
    top7_titles.groupby("movie_title")["Daily"].sum().sort_values().index
)
plot_order = {title: order for order, title in enumerate(plot_order)}
# %%

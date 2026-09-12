import os

import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not configured")


engine = create_engine(DATABASE_URL)


def load_data():

    query = """
        SELECT
            city,
            country,
            latitude,
            longitude,
            temperature,
            feels_like,
            min_temperature,
            max_temperature,
            pressure,
            humidity,
            wind_speed,
            cloudiness,
            weather_condition,
            weather_description,
            collection_time,
            collection_date
        FROM weather_data
        ORDER BY collection_time
    """

    df = pd.read_sql(query, engine)

    df["collection_time"] = pd.to_datetime(
        df["collection_time"],
        errors="coerce"
    )

    df["collection_date"] = pd.to_datetime(
        df["collection_date"],
        errors="coerce"
    )

    return df


df = load_data()


app = Dash(__name__)

app.title = "Weather EDA Dashboard"


city_options = [
    {"label": city, "value": city}
    for city in sorted(df["city"].dropna().unique())
]


app.layout = html.Div(
    [

        html.H1(
            "WEATHER DATA ANALYSIS DASHBOARD",
            style={"textAlign": "center"}
        ),

        html.Div(
            [

                html.Div(
                    [
                        html.Label("Select City"),

                        dcc.Dropdown(
                            id="city-filter",
                            options=city_options,
                            value=city_options[0]["value"]
                            if city_options
                            else None,
                            clearable=False
                        )
                    ],
                    style={
                        "width": "30%",
                        "display": "inline-block",
                        "marginRight": "20px"
                    }
                ),

                html.Div(
                    [
                        html.Label("Select Date Range"),

                        dcc.DatePickerRange(
                            id="date-filter",
                            min_date_allowed=df["collection_date"].min(),
                            max_date_allowed=df["collection_date"].max(),
                            start_date=df["collection_date"].min(),
                            end_date=df["collection_date"].max()
                        )
                    ],
                    style={
                        "width": "45%",
                        "display": "inline-block"
                    }
                )

            ],
            style={"marginBottom": "25px"}
        ),

        html.Div(
            [

                html.Div(
                    [
                        html.H4("Average Temperature"),
                        html.H2(id="avg-temperature")
                    ],
                    className="card"
                ),

                html.Div(
                    [
                        html.H4("Average Humidity"),
                        html.H2(id="avg-humidity")
                    ],
                    className="card"
                ),

                html.Div(
                    [
                        html.H4("Average Wind Speed"),
                        html.H2(id="avg-wind")
                    ],
                    className="card"
                ),

                html.Div(
                    [
                        html.H4("Total Records"),
                        html.H2(id="total-records")
                    ],
                    className="card"
                )

            ],
            style={
                "display": "flex",
                "gap": "20px",
                "marginBottom": "30px"
            }
        ),

        dcc.Graph(id="temperature-trend"),

        html.Div(
            [

                html.Div(
                    dcc.Graph(id="humidity-trend"),
                    style={
                        "width": "49%",
                        "display": "inline-block"
                    }
                ),

                html.Div(
                    dcc.Graph(id="wind-trend"),
                    style={
                        "width": "49%",
                        "display": "inline-block"
                    }
                )

            ]
        ),

        html.Div(
            [

                html.Div(
                    dcc.Graph(id="weather-condition"),
                    style={
                        "width": "49%",
                        "display": "inline-block"
                    }
                ),

                html.Div(
                    dcc.Graph(id="cloudiness-chart"),
                    style={
                        "width": "49%",
                        "display": "inline-block"
                    }
                )

            ]
        ),

        dcc.Graph(id="correlation-heatmap")

    ],
    style={
        "padding": "25px",
        "fontFamily": "Arial"
    }
)


@app.callback(

    [
        Output("avg-temperature", "children"),
        Output("avg-humidity", "children"),
        Output("avg-wind", "children"),
        Output("total-records", "children"),
        Output("temperature-trend", "figure"),
        Output("humidity-trend", "figure"),
        Output("wind-trend", "figure"),
        Output("weather-condition", "figure"),
        Output("cloudiness-chart", "figure"),
        Output("correlation-heatmap", "figure")
    ],

    [
        Input("city-filter", "value"),
        Input("date-filter", "start_date"),
        Input("date-filter", "end_date")
    ]
)


def update_dashboard(
    selected_city,
    start_date,
    end_date
):

    filtered_df = df.copy()

    if selected_city:

        filtered_df = filtered_df[
            filtered_df["city"] == selected_city
        ]

    if start_date:

        filtered_df = filtered_df[
            filtered_df["collection_date"]
            >= pd.to_datetime(start_date)
        ]

    if end_date:

        filtered_df = filtered_df[
            filtered_df["collection_date"]
            <= pd.to_datetime(end_date)
        ]

    if filtered_df.empty:

        empty_fig = px.scatter(
            title="No data available"
        )

        return (
            "0",
            "0",
            "0",
            "0",
            empty_fig,
            empty_fig,
            empty_fig,
            empty_fig,
            empty_fig,
            empty_fig
        )

    avg_temperature = filtered_df["temperature"].mean()

    avg_humidity = filtered_df["humidity"].mean()

    avg_wind = filtered_df["wind_speed"].mean()

    total_records = len(filtered_df)

    temperature_fig = px.line(
        filtered_df,
        x="collection_time",
        y=["temperature", "feels_like"],
        title="Temperature Trend",
        labels={
            "value": "Temperature (°C)",
            "collection_time": "Time"
        }
    )

    temperature_fig.update_layout(
        hovermode="x unified"
    )

    humidity_fig = px.line(
        filtered_df,
        x="collection_time",
        y="humidity",
        title="Humidity Trend",
        labels={
            "humidity": "Humidity (%)",
            "collection_time": "Time"
        }
    )

    wind_fig = px.line(
        filtered_df,
        x="collection_time",
        y="wind_speed",
        title="Wind Speed Trend",
        labels={
            "wind_speed": "Wind Speed",
            "collection_time": "Time"
        }
    )

    condition_counts = (
        filtered_df["weather_condition"]
        .value_counts()
        .reset_index()
    )

    condition_counts.columns = [
        "weather_condition",
        "count"
    ]

    condition_fig = px.bar(
        condition_counts,
        x="weather_condition",
        y="count",
        title="Weather Condition Distribution",
        labels={
            "weather_condition": "Condition",
            "count": "Records"
        }
    )

    cloudiness_fig = px.histogram(
        filtered_df,
        x="cloudiness",
        nbins=20,
        title="Cloudiness Distribution",
        labels={
            "cloudiness": "Cloudiness (%)"
        }
    )

    numeric_columns = [
        "temperature",
        "feels_like",
        "min_temperature",
        "max_temperature",
        "pressure",
        "humidity",
        "wind_speed",
        "cloudiness"
    ]

    correlation_df = filtered_df[
        numeric_columns
    ].corr()

    correlation_fig = px.imshow(
        correlation_df,
        text_auto=True,
        title="Weather Variable Correlation",
        aspect="auto"
    )

    return (
        f"{avg_temperature:.2f} °C",
        f"{avg_humidity:.2f} %",
        f"{avg_wind:.2f}",
        f"{total_records:,}",
        temperature_fig,
        humidity_fig,
        wind_fig,
        condition_fig,
        cloudiness_fig,
        correlation_fig
    )


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=8050,
        debug=False
    )
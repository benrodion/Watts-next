# Watts next? Time series analysis for wind energy production in the Netherlands.
Project prepared by MDS 2026 class students of Hertie School for the Machine Learning course.

Group: Benedict Anderer, Sofiya Berdiyeva, Polina Ianina, Kirill Kharlashkin, Franka Tetteroo.

## Summary
The primary goal of this project was to predict future wind power generation for both **offshore and onshore** Dutch wind farms. The overarching motivation was to improve grid management and mitigate issues like curtailment and redispatch, which arise from the volatility of wind power and pose challenges to the energy transition. Mean Squared Error was the main performance measure used to evaluate the models.

## Data Sources

The project utilised several data sources:

*   **Wind Energy Production Data** retrieved from the [**National Energy Dashboard (NED)**](https://ned.nl/). This included hourly data on offshore and onshore wind energy production in the Netherlands from 2017 onwards.
*   **Weather Data** were obtained from [**The Royal Netherlands Meteorological Institute (KNMI)**](https://www.daggegevens.knmi.nl/klimatologie/uurgegevens). Key [variables](https://english.knmidata.nl/open-data/actuele10mindataknmistations) included wind direction, hourly average wind speed, wind speed averaged over 10 minutes, air pressure, and relative humidity. Data were collected for all stations, that were later manually categorised as offshore and onshore.
*   **Price Data** were downloaded from the website [**Jeroen.nl**](Jeroen.nl). They provide dynamic electricity prices for consumers and were included only as an experimental feature.

Hourly data were aggregated as average values per day, week and month to run the models with different granularities.

## Models

*   **Baseline Models:**
    *   **Holt-Winters exponential smoothing** was adjusted for additive trend and seasonality.
    *   **SARIMAX** was adjusted for seasonal autoregressive terms, trend and seasonal differencing, and fitted with scaled exogenous variables.
    *   **Ensemble model** combining **Polynomial regression** and **Random Forest Regressor** was trained with all exogenous weather variables, aiming to capture global fluctuations and recent short-term dynamics.

*   **Complex Models:**
    *   **XGBoost** explored variations including using the log of wind production as the label and including price data as a feature.
    *   **Prophet (from Facebook/Meta)** was applied to monthly, weekly, and daily aggregates.
    *   **Chronos** was used for monthly and weekly aggregates.

## License

[MIT](https://choosealicense.com/licenses/mit/)

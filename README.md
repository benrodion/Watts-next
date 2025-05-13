# Watts next? Time series analysis for wind energy production in the Netherlands.
Project prepared by MDS 2026 class students of Hertie School for the Machine Learning course.
Group: Benedict Anderer, Sofiya Berdiyeva, Polina Ianina, Kirill Kharlashkin, Franka Tetteroo.

## Summary
The primary goal of this project is to predict future wind power generation for both **offshore and onshore** Dutch wind farms. The overarching motivation is to improve grid management and mitigate issues like curtailment and redispatch, which arise from the volatility of wind power and pose challenges to the energy transition. The main performance measure used to evaluate the models is the Mean Squared Error.

## Data Sources

The project utilised several data sources:

*   **Wind Energy Production Data:** Retrieved from the [**National Energy Dashboard (NED)**](https://ned.nl/). This included hourly data on offshore and onshore wind energy production in the Netherlands from 2017 onwards.
*   **Weather Data:** Obtained from [**The Royal Netherlands Meteorological Institute (KNMI)**](https://www.daggegevens.knmi.nl/klimatologie/uurgegevens). Key [variables](https://english.knmidata.nl/open-data/actuele10mindataknmistations) included **wind direction, hourly average wind speed, wind speed averaged over 10 minutes, air pressure, and relative humidity**. Data was collected for multiple stations and categorised for offshore and onshore analysis.
*   **Price Data:** Downloaded from the website [**Jeroen.nl**](Jeroen.nl), providing dynamic electricity prices for consumers. Included only as an experimental feature.

## Models

*   **Baseline Models:**
    *   **Holt-Winters exponential smoothing**: Adjusted for additive trend and seasonality.
    *   **SARIMAX**: Adjusted for seasonal autoregressive terms, trend and seasonal differencing, and fitted with scaled exogenous variables.
    *   **Ensemble model** combining **Polynomial regression** and **Random Forest Regressor**: Trained with all exogenous weather variables, aiming to capture global fluctuations and recent short-term dynamics.

*   **Complex Models:**
    *   **XGBoost**: Explored variations including using the log of wind production as the label and including price data as a feature.
    *   **Prophet (from Facebook/Meta)**: Applied to monthly, weekly, and daily aggregates.
    *   **Chronos**: Used for monthly and weekly aggregates.

These models were applied and evaluated across different data granularities: monthly, weekly, daily, and hourly.

## License

[MIT](https://choosealicense.com/licenses/mit/)

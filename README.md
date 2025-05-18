# Watts next? Time series analysis for wind energy production in the Netherlands.
Project prepared by MDS 2026 class students of Hertie School for the Machine Learning course.

Group: [Benedict Anderer](https://github.com/benrodion), Sofiya Berdiyeva, Polina Ianina, Kirill Kharlashkin, Franka Tetteroo.

## Summary
The primary goal of this project was to predict future wind power generation for both **offshore and onshore** Dutch wind farms. The overarching motivation was to improve grid management and mitigate issues like curtailment and redispatch, which arise from the volatility of wind power and pose challenges to the energy transition. Mean Squared Error was the main performance measure used to evaluate the models.

## Data Sources

The project utilised several data sources:

*   **Wind Energy Production Data** retrieved from the [**National Energy Dashboard (NED)**](https://ned.nl/). This included hourly data on offshore and onshore wind energy production in the Netherlands from 2017 onwards.
*   **Weather Data** were obtained from [**The Royal Netherlands Meteorological Institute (KNMI)**](https://www.daggegevens.knmi.nl/klimatologie/uurgegevens). Key [variables](https://english.knmidata.nl/open-data/actuele10mindataknmistations) included wind direction, hourly average wind speed, wind speed averaged over 10 minutes, air pressure, and relative humidity. Data were collected for all stations, that were later manually categorised as offshore and onshore.
*   **Price Data** were downloaded from the website [**Jeroen.nl**](Jeroen.nl). They provide dynamic electricity prices for consumers and were included only as an experimental feature.

Hourly data were aggregated as average values per day, week and month to run the models with different granularities.

## Repo Structure
The repo is structured as follows: the 'data'-folder contains the data we worked with, 'EDA' contains the exploratory data analysis, 'models' is where the notebooks in which we developed the models are located, 'plots' contains the plots we used for our presentation, and 'visualization' contains the code with which we created our visuals. 'old' is a folder with material we did not include in the final project. 
```
├──  readings on ML for WE prediction
│   ├── Predicting WE generation using meteo data Turkiye.pdf
│   ├── Summary_readings.docx
│   ├── Wind power prediction ML models.pdf
│   └── Wind power prediction using wind speed.pdf
├── data
│   ├── final_offshore_data_2017_2025.csv
│   ├── final_onshore_data_2017_2025.csv
│   ├── Offshore_official_data_incl_prices.csv
│   ├── offshore_rf.csv
│   ├── Onshore_official_data_incl_prices.csv
│   ├── preparation
│   │   ├── avg_offshore_meteo_2017_2025.csv
│   │   ├── avg_onshore_meteo_2017_2025.csv
│   │   ├── ElectricityPricesDataRetrieval.ipynb
│   │   ├── KNMI_data_retrieval.ipynb
│   │   ├── meteo_data_retrieval.py
│   │   ├── WindGeneration_onshore_2017_2025.csv
│   │   ├── WindGenerationDataRetrieval.ipynb
│   │   ├── WindOffShore_data_2017_2025_clean.csv
│   │   ├── WindOffShore_data_2017_2025.csv
│   │   └── WindOnShore_data_2017_2025_clean.csv
│   ├── Stations_map.pbix
│   └── Stations.csv
├── EDA
│   ├── EDA.ipynb
│   └── EDA.pdf
├── helpers.py
├── LICENSE
├── models
│   ├── model_development.ipynb
│   ├── model_research.ipynb
│   ├── models_chronos.ipynb
│   ├── models_ensemble.ipynb
│   ├── models_prophet.ipynb
│   ├── models_sarimax.ipynb
│   ├── sandbox_model_random_forest.ipynb
│   ├── TS + HW + SARIMAX_very final.ipynb
│   ├── TS analysis + HW + SARIMAX_final.ipynb
│   ├── XGBoostAllOffshore-3.ipynb
│   ├── XGBoostAllOffshore.ipynb
│   ├── XGBoostAllOnshore.ipynb
│   ├── XGBoostPriceOffshore.ipynb
│   └── XGBoostPriceOnshore.ipynb
├── old
│   ├── LSTM.ipynb
│   └── models_LSTM.ipynb
├── plots
│   ├── ensemble_off_d.png
│   ├── ensemble_off_h.png
│   ├── ensemble_off_m.png
│   ├── ensemble_off_w.png
│   ├── ensemble_on_d.png
│   ├── ensemble_on_h.png
│   ├── ensemble_on_m.png
│   ├── ensemble_on_w.png
│   ├── off_d.png
│   ├── off_h.png
│   ├── off_m.png
│   ├── off_w.png
│   ├── on_d.png
│   ├── on_h.png
│   ├── on_m.png
│   ├── on_w.png
│   ├── upd_off_d.png
│   ├── upd_off_h.png
│   ├── upd_off_m.png
│   ├── upd_off_w.png
│   ├── upd_on_d.png
│   ├── upd_on_h.png
│   ├── upd_on_m.png
│   └── upd_on_w.png
├── readings on ML for WE prediction
│   ├── Summary_readings.docx
│   └── Wind power prediction ML models.pdf
├── README.md
└── visualization
    ├── models_chronos graph.ipynb
    ├── models_prophet graph.ipynb
    ├── Results_visuals_new.ipynb
    └── Results_visuals.ipynb
```
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

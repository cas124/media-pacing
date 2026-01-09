# test_google_meridian

# ==================================================================================== #
#### STEP 0: INSTALL ####
# ==================================================================================== #

import arviz as az
import IPython
from meridian import constants
from meridian.analysis import analyzer
from meridian.analysis import formatter
from meridian.analysis import optimizer
from meridian.analysis import summarizer
from meridian.analysis import visualizer
from meridian.data import data_frame_input_data_builder
from meridian.data import test_utils
from meridian.model import model
from meridian.model import prior_distribution
from meridian.model import spec
import numpy as np
import pandas as pd
# check if GPU is available
from psutil import virtual_memory
import tensorflow as tf
import tensorflow_probability as tfp

ram_gb = virtual_memory().total / 1e9
print('Your runtime has {:.1f} gigabytes of available RAM\n'.format(ram_gb))
print(
    'Num GPUs Available: ',
    len(tf.config.experimental.list_physical_devices('GPU')),
)
print(
    'Num CPUs Available: ',
    len(tf.config.experimental.list_physical_devices('CPU')),
)

df = pd.read_csv(
    "https://raw.githubusercontent.com/google/meridian/refs/heads/main/meridian/data/simulated_data/csv/geo_all_channels.csv"
)

builder = data_frame_input_data_builder.DataFrameInputDataBuilder(
    kpi_type='non_revenue',
    default_kpi_column='conversions',
    default_revenue_per_kpi_column='revenue_per_conversion',
)

# ==================================================================================== #
#### STEP 1: LOAD THE SIMULATOR DATA ####
# ==================================================================================== #

builder = (
    builder.with_kpi(df)
    .with_revenue_per_kpi(df)
    .with_population(df)
    .with_controls(
        df, control_cols=["sentiment_score_control", "competitor_sales_control"]
    )
)

channels = ["Channel0", "Channel1", "Channel2", "Channel3", "Channel4"]
builder = builder.with_media(
    df,
    media_cols=[f"{channel}_impression" for channel in channels],
    media_spend_cols=[f"{channel}_spend" for channel in channels],
    media_channels=channels,
)

builder = builder.with_non_media_treatments(
    df, non_media_treatment_cols=['Promo']
).with_organic_media(
    df,
    organic_media_cols=['Organic_channel0_impression'],
    organic_media_channels=['Organic_channel0'],
)

data = builder.build()

## Note that the simulated data here does not contain reach and frequency. We recommend including reach and frequency data whenever they are available


# ==================================================================================== #
#### STEP 3: CONFIGURE THE MODEL ####
# ==================================================================================== #

roi_mu = 0.2  # Mu for ROI prior for each media channel.
roi_sigma = 0.9  # Sigma for ROI prior for each media channel.
prior = prior_distribution.PriorDistribution(
    roi_m=tfp.distributions.LogNormal(roi_mu, roi_sigma, name=constants.ROI_M)
)
model_spec = spec.ModelSpec(prior=prior, enable_aks=True)

mmm = model.Meridian(input_data=data, model_spec=model_spec)

%%time
mmm.sample_prior(500)
mmm.sample_posterior(
    n_chains=10, n_adapt=2000, n_burnin=500, n_keep=1000, seed=0
)



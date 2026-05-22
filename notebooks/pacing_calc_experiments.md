---
title: Pacing Calc Experiments
marimo-version: 0.23.7
width: medium
---

```python {.marimo}
import marimo as mo

import polars as pl
import matplotlib.pyplot as plt
import numpy as np

from strava_history_analysis import get_spine, get_time_series
from strava_history_analysis.time_series_functions import (
    compute_peak_normalized_power,
    compute_peak_average_power,
    compute_average_power,
    compute_normalized_power,
)
```

## Building the dataset to fit the pacing calculator

1. For each ride, compute average power for entire ride, as well peak average power for 1m, 5m, 10m, 20m, 60m, 120m, as well as peak 1h and 2h NP.
2. We will use the peak 1h and 2h NP, as well as the whole ride average power as point measurements, and the rest as censored measurements.

```python {.marimo}
poll_strava_boolean = mo.ui.switch(value=False, label="Pull latest data from Strava")
poll_strava_boolean
```

```python {.marimo}
df = get_spine(root_path="./", poll_strava=poll_strava_boolean.value).select(
    [
        pl.col("Activity ID"),
        pl.col("Activity Date"),
        pl.col("Activity Type"),
        pl.col("Activity Name"),
        pl.col("Activity Gear"),
        pl.col("Elapsed Time"),
        pl.col("Moving Time"),
        pl.col("Filename"),
    ]
)
```

```python {.marimo}
root_path = "./"

dfnp = df.with_columns(
    [
        pl.col("Filename")
        .map_elements(
            lambda f: compute_peak_normalized_power(3600, f, root_path),
            return_dtype=pl.Float64,
        )
        .alias("Peak 1h normalized power"),
        pl.col("Filename")
        .map_elements(
            lambda f: compute_peak_normalized_power(7200, f, root_path),
            return_dtype=pl.Float64,
        )
        .alias("Peak 2h normalized power"),
        pl.col("Filename")
        .map_elements(
            lambda f: compute_peak_average_power(60, f, root_path),
            return_dtype=pl.Float64,
        )
        .alias("Peak 1m average power"),
        pl.col("Filename")
        .map_elements(
            lambda f: compute_peak_average_power(300, f, root_path),
            return_dtype=pl.Float64,
        )
        .alias("Peak 5m average power"),
        pl.col("Filename")
        .map_elements(
            lambda f: compute_peak_average_power(600, f, root_path),
            return_dtype=pl.Float64,
        )
        .alias("Peak 10m average power"),
        pl.col("Filename")
        .map_elements(
            lambda f: compute_peak_average_power(1200, f, root_path),
            return_dtype=pl.Float64,
        )
        .alias("Peak 20m average power"),
        pl.col("Filename")
        .map_elements(
            lambda f: compute_peak_average_power(3600, f, root_path),
            return_dtype=pl.Float64,
        )
        .alias("Peak 60m average power"),
        pl.col("Filename")
        .map_elements(
            lambda f: compute_peak_average_power(7200, f, root_path),
            return_dtype=pl.Float64,
        )
        .alias("Peak 120m average power"),
        pl.col("Filename")
        .map_elements(
            lambda f: compute_average_power(f, root_path),
            return_dtype=pl.Float64,
        )
        .alias("Average power"),
        pl.col("Filename")
        .map_elements(
            lambda f: compute_normalized_power(f, root_path),
            return_dtype=pl.Float64,
        )
        .alias("Normalized power"),
    ]
)
```

```python {.marimo}
f_valid = (
    (pl.col("Peak 1h normalized power").is_finite())
)
```

```python {.marimo}
dfnpf = dfnp.filter(f_valid)#.tail(10)
```

```python {.marimo}
from strava_history_analysis.pacing_calculator import PacingModel
```

```python {.marimo}
# Initial estimates
anaerobic_power = 373
tau = 0.6
alpha = 0.054
watts_scaling_factor = 224
cov = np.diag([100**2, 30**2])
stickiness = 58
```

```python {.marimo}
baseline_model = PacingModel(
    anaerobic_work=anaerobic_power,
    watts_scaling_factor=watts_scaling_factor,
    covariance_matrix=cov,
    tau=tau,
    alpha=alpha,
    stickiness=stickiness,
)
```

```python {.marimo}
baseline_model.predict_peak_power(5)
```

```python {.marimo}
p5s = []
p20s = []
p60s = []

for activity in dfnpf.iter_rows(named=True):
    censored_observations = []
    uncensored_observations = []

    censored_observations.append((5, activity['Peak 5m average power']))
    censored_observations.append((10, activity['Peak 10m average power']))
    censored_observations.append((20, activity['Peak 20m average power']))
    censored_observations.append((60, activity['Peak 60m average power']))
    censored_observations.append((120, activity['Peak 120m average power']))

    uncensored_observations.append((60, activity['Peak 1h normalized power']))
    uncensored_observations.append((120, activity['Peak 2h normalized power']))
    uncensored_observations.append((activity["Moving Time"] / 60, activity['Normalized power']))

    censored_observations = list(filter(lambda f: f[1] is not None, censored_observations))
    uncensored_observations = list(filter(lambda f: f[1] is not None, uncensored_observations))

    baseline_model.update_based_on_observations(
        censored_observations,
        uncensored_observations
    )
    p5 = baseline_model.predict_peak_power(5)
    p20 = baseline_model.predict_peak_power(20)
    p60 = baseline_model.predict_peak_power(60)

    p5s.append(p5)
    p20s.append(p20)
    p60s.append(p60)

    # print(f"5m = {p5}, 20m = {p20}, 60m = {p60}")
```

```python {.marimo}
print(baseline_model)
```

```python {.marimo}
baseline_model.predict_peak_power(60 * 17)
```

```python {.marimo}
from datetime import datetime, timezone
f_recent = dfnpf.get_column("Activity Date") >= datetime(2024, 3, 1, tzinfo=timezone.utc)
```

```python {.marimo}
dfnpf_additional = dfnpf.with_columns(
    pl.Series("Predicted 5m power", p5s),
    pl.Series("Predicted 20m power", p20s),
    pl.Series("Predicted 60m power", p60s),
)
```

```python {.marimo}
plt.figure(figsize=(20, 10))

for t in [5, 
          # 20, 
          # 60,
         ]:
    plt.plot(
        dfnpf_additional.filter(f_recent)["Activity Date"],
        dfnpf_additional.filter(f_recent)[f"Predicted {t}m power"],
        label=f"Predicted {t}m power"
    )

plt.legend()
plt.grid()

plt.xlabel("Date of prediction")
plt.ylabel("Power (W)")

plt.gca()
```

```python {.marimo}

```
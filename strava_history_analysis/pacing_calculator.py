"""
# Outline of the calculator

The key idea here is to maintain an internal power-duration curve that is constantly updating, across the time axis
as I collect more and more ride data. It should also update the historical data points to account for any change in fitness
over time.

I'm planning on using a fairly simple functional form for the power curve, and I will fit the params off of my data.
The key requirement I have from this model is that it does well in the 6hr+ regime, since I'm mostly interested in doing
ultras. Once I do some multi-day events, I might have to change to model to account for that as well.

```
P(t) = A / (t + τ) + B * t^(-α)
```
### Parameters

- **A** (joules): Anaerobic work capacity contribution
- **τ** (seconds): Time constant that prevents singularity at t=0 and shapes short-duration behavior
- **B** (watts × seconds^α): Scaling factor for the aerobic/endurance component
- **α** (dimensionless): Decay exponent, typically 0.05-0.10

# Fitting the model

- We first initialize the pacing calculator with some reasonable defaults.
- For A and B, we assume that the parameters are normally distributed, with some mean and stdev the calculator will fit
- For each activity in our time series, we measure the peak average power over various subintervals
- We also measure HR over those intervals, and use that as a proxy for whether the measurement is an exact measurement or a lower bound.

The function we will try to minimize for each activity will be either regular log-likelihood, or a censored version of log-likelihood.
It will be up to the user to label a measurement as censored or not. Hopefully a few uncensored measurements should be good enough to prevent
drift of mass.

Alternatively, we can try and minimize a linear combination of the censored and uncensored versions, where the weights are some function of heart rate
zscore.
"""

from dataclasses import dataclass
import numpy as np
from numpy.typing import NDArray
from typing import List, Tuple
from scipy.optimize import minimize
from scipy.stats import norm, multivariate_normal


@dataclass
class PacingModel:
    anaerobic_work: float  # joules, this is something we update online
    watts_scaling_factor: float  # this is also something we update online
    covariance_matrix: NDArray[
        np.float64
    ]  # This is the covariance matrix of the the params
    tau: float  # This is a fixed constant as the model gets in more data
    alpha: float  # decay exponent, also fixed

    def predict_peak_power(
        self,
        duration: int,  # duration in minutes
        overridden_anaerobic_work=None,
        overridden_watts_scaling_factor=None,
    ):
        if overridden_anaerobic_work is None:
            overridden_anaerobic_work = self.anaerobic_work
        if overridden_watts_scaling_factor is None:
            overridden_watts_scaling_factor = self.watts_scaling_factor

        return (overridden_anaerobic_work) / (
            duration + self.tau
        ) + overridden_watts_scaling_factor * (duration) ** (-1 * self.alpha)

    def update_based_on_observations(
        self,
        censored_observations: List[Tuple[int, float]],
        uncensored_observations: List[Tuple[int, float]],
        noise_floor_A: float = 100.0,  # std of 10 W·min
        noise_floor_B: float = 25.0,   # std of 5
    ):
        """
        This function takes two lists of pairs of duration and power, one of which are censored observations,
        and the other are uncensored observations, and updates the anaerobic work, watts_scaling_factor, and the
        covariance_matrix attributes.
        """
        prior_mean = np.array([self.anaerobic_work, self.watts_scaling_factor])
        prior_cov = self.covariance_matrix

        def obs_sigma(duration):
            j = np.array([1.0 / (duration + self.tau), duration ** (-self.alpha)])
            return np.sqrt(j @ prior_cov @ j)

        def neg_log_posterior(params):
            A, B = params

            ll = 0.0
            for duration, power in uncensored_observations:
                ll += norm.logpdf(
                    power,
                    self.predict_peak_power(duration, A, B),
                    scale=obs_sigma(duration),
                )
            for duration, power in censored_observations:
                ll += norm.logsf(
                    power,
                    self.predict_peak_power(duration, A, B),
                    scale=obs_sigma(duration),
                )

            log_prior = multivariate_normal.logpdf(
                params, mean=prior_mean, cov=prior_cov
            )

            return -(ll + 2 * log_prior)

        result = minimize(neg_log_posterior, x0=prior_mean, method="BFGS")

        self.anaerobic_work, self.watts_scaling_factor = result.x
        self.covariance_matrix = result.hess_inv + np.diag([noise_floor_A, noise_floor_B])

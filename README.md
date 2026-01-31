# Strava History Analysis

This is a hobby project that I am using to satisfy several needs at once.

1. Getting my data out of Strava just in case I no longer wish to use it post its IPO.
2. Run complex queries over my historical activities: while I can easily see the metrics I care about for each activity individually on Strava, I would like to able to work with the entire dataset as whole, and analyze it in a systematic manner.
3. Build a simple power pacing calculator for my really long rides, especially if the rides are longer than anything I've done in the past (details on that below). 
4. An excuse to use Polars, and do quant like work on my own data (as opposed to data at work).


## Pacing calculator

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
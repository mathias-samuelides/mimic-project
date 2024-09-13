# MIMIC-IV Data Pipeline Refactoring

## Objective

This project aims to refactor the data processing pipeline for MIMIC-IV, originally based on the repository [MIMIC-IV Data Pipeline](https://github.com/healthylaife/MIMIC-IV-Data-Pipeline).

The main goal is to **restructure the code** while keeping the same functionalities. The focus is not on reproducing the exact same results or optimizing computations at this stage. Instead, the emphasis is on **reorganizing** and **simplifying** the code to make it more readable and maintainable.

The cohort extraction and feature engineering parts have been **re-implemented**.

## Citation

This work is inspired by the pipeline presented in the paper:

> Gupta, M., Gallamoza, B., Cutrona, N., Dhakal, P., Poulain, R., & Beheshti, R. (2022). An Extensive Data Processing Pipeline for MIMIC-IV. Proceedings of the 2nd Machine Learning for Health symposium, PMLR, 193, 311–325. [Link to paper](https://proceedings.mlr.press/v193/gupta22a.html)

## Data

We are using **test/sample data** available at:

[https://physionet.org/content/mimic-iv-demo/2.2/](https://physionet.org/content/mimic-iv-demo/2.2/)

1. Create a directory to store the data:

    ```bash
    mkdir data
    ```

2. Place the MIMIC-IV data in the following directory:

    ```bash
    mkdir data/mimiciv_2_0
    ```

3. Place the static data for code mappings in the following directory:

    ```bash
    mkdir data/mappings
    ```

## Installation

1. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

2. Run the tests to verify the setup:

    ```bash
    python -m pytest
    ```

## Work in Progress

This project is a **work in progress**, and the pipeline is actively being refactored to improve code structure without affecting the overall functionality.

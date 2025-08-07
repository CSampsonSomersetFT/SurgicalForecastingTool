# Surgical Forecasting Tool

A forecasting tool designed to support surgical scheduling and capacity planning within healthcare settings. This project leverages data science techniques to predict surgical demand and optimize resource allocation.

## 📦 Repository Structure
- `data/`: Contains input datasets used for modeling and evaluation.
- `results/`: Output files including forecasts and performance metrics.
- `surgical_sim/`: Contains the code used to power the simulation.
- `experiment_scenarios.ipynb`: Contains code used to create the results files.
- `data_analysis_validation.ipynb`: Contains code used to validate the simulation.
- `sim_validation.ipynb`: Further validation using full random selection.
- `results_analysis.ipynb`: Analysis of results.
- `simulation_framework_simpy.ipynb`: Rough workings used to create the module.
- `simulation_parameterisation.ipynb`: Processing of the parameterisation data.


## 🛠️ Installation

```bash
# Clone the repository
git clone https://github.com/CSampsonSomersetFT/SurgicalForecastingTool.git
cd SurgicalForecastingTool

# (Optional) Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`

# Install dependencies
pip install -r requirements.txt

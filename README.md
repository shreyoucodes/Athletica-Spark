# 🏅 Athlete Medal Points Aggregator

This project analyzes Olympic athlete data across multiple years (2012, 2016, 2020) and calculates total medal points for each athlete.

## 📁 Folder Structure

```bash
Athletica-Spark
├── athletes_2012.csv
├── athletes_2012_large.csv
├── athletes_2016.csv
├── athletes_2016_large.csv
├── athletes_2020.csv
├── athletes_2020_large.csv
├── coaches.csv
├── coaches_large.csv
├── medals.csv
├── medals_large.csv
├── output.txt
├── expected_output_large.txt
├── expected_output_small.txt
├── task1.py # ✅ Main program file
└── task1_extra.py
```


## 🚀 Task Overview

### Goal:
To compute and rank athletes based on their **total medal points** using their participation across three Olympic years.

### Workflow:
1. Read datasets for athletes (2012, 2016, 2020) and medals.
2. For each year:
   - Create a medal dataframe with assigned points.
   - Join the athlete dataframe with medal dataframe using a common key.
3. Merge yearly data and calculate **total points** for each athlete (`id`, `name`).
4. Sort the athletes in **descending** order of points.
5. Output the results to `output.txt`.

## 🏆 Medal Points

| Medal  | Points |
|--------|--------|
| Gold   | 3      |
| Silver | 2      |
| Bronze | 1      |

## 🛠️ Requirements

- Python 3.x
- pandas

Install dependencies:
```bash
pip install pandas
```

## 🧪 Run the Code
```bash
python task1.py
```
Output will be stored in output.txt.


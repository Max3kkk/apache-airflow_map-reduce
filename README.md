# Counting word frequencies in tweets, following MapReduce programming model in **Apachy Airflow**
## Prerequisites
* pip install -r requirements.txt
### Files definitions:

- [dags](dags) - Path with DAG file
- [plugins](plugins) - Path with custom operators(MapOperator and ReduceOperator)
- [tweets.csv](tweets.csv) - Tweets Dataset - Top 20 most followed users in Twitter social platform :(https://dataverse.harvard.edu/dataset.xhtml?id=3047332)
- [word_frequency.csv](word_frequency.csv) - File with word frequencies

## To launch:

```console
    pip install -r requirements.txt
    
    airflow standalone

    # Visit localhost:8080 in the browser and use the admin account details
    # shown on the terminal to login.
    # Enable the example_bash_operator dag in the home page
```
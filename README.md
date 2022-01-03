# Twitter Data Streaming Project

![Twitter](https://img.shields.io/badge/Twitter-1DA1F2?style=for-the-badge&logo=twitter&logoColor=white)
![GoogleCloud](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

<br>
[![GitHub license](https://img.shields.io/github/license/Naereen/StrapDown.js.svg)](https://github.com/Naereen/StrapDown.js/blob/master/LICENSE)


This is the repository for the project Twitter data streaming analytics using Spark Streaming and Latent Dirichlet allocation (LDA). Twitter data is captured on gcp. 
A socket requests data from Twitter API and sends data to the Spark streaming
process. Spark reads real-time data to do analysis. It also saves temporary streaming results
to Google Storage. After the streaming process terminates, Spark reads the final data
from Google Storage and saves it to BigQuery, and then cleans the data in Storage.
Finally, LDA is used to classify the data in the streaming.

## Architecture
![Alt text](https://raw.githubusercontent.com/aqid98/twitterStreamingProject/main/images/architectureTwitter.PNG?raw=true "Architecture")

## Run Instructions

Start streaming
1. Run twitterHTTPClient.ipynb
2. Run sparkStreaming.ipynb
3. You can test sparkStreaming.ipynb multiple times and leave twitterHTTPClient.ipynb running
4. Stop twitterHTTPClient.ipynb




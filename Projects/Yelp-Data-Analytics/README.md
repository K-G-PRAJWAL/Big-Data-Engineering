## Yelp Data Analytics

Yelp businesses dataset is bulk loaded into Elasticsearch index. A kafka producer provides streaming data of yelp reviews which is consumed into MongoDB. The data being loaded into MongoDB is streamed back into Kafka through a Kafka connector which would also have the review sentiment after being analysed through a transformer huggingface model(distilbert-base-uncased-finetuned-sst-2-english). This data streamed back into Kafka is loaded into elasticserach index for reviews. A kibana dashboard is built on top of this to provide visualization to various fields available in the dataset.

Elasticsearch Deployment: https://cloud.elastic.co/deployments/55f47470ebe24f7eb6ecfd7dc8b21fe6

Elaticsearch Cluster: https://yelp-deployment.kb.ap-south-1.aws.elastic-cloud.com/app/elasticsearch/overview

Elasticsearch Index: https://yelp-deployment.kb.ap-south-1.aws.elastic-cloud.com/app/elasticsearch/indices/index_details/businesses/data

Kafka Cluster: https://confluent.cloud/environments/env-qz7m5m/clusters/lkc-p1pvk2/api-keys

MongoDB Cluster: https://cloud.mongodb.com/v2/6820aec5ffad0c513896fca0#/overview?automateSecurity=true

##### Arch Diagram

![](https://github.com/K-G-PRAJWAL/Big-Data-Engineering/tree/main/Projects/Yelp-Data-Anlaytics/Images/arch_diagram.png)

def preprocess(bucket_name):
    import os

    import pandas as pd
    import numpy as np
    import boto3

    region = boto3.session.Session().region_name  # set the region

    # Output path for saving the model
    prefix = "xgboost"
    output_path = "s3://{}/{}/output".format(bucket_name, prefix)

    # Download S3 data and load into pandas df
    prefix_raw = "raw"
    file_name = "ml_train_data.csv"
    data_location = "s3://{}/{}/{}".format(bucket_name, prefix_raw, file_name)

    df = pd.read_csv(data_location)  # Read CSV into df

    # "Pickup_datetime" is an object, convert to "datetime_object"
    df['pickup_datetime'] = pd.to_datetime(
        df['pickup_datetime'], format='%Y-%m-%d %H:%M:%S UTC')

    # Ensure the geographical location of interest is only New York
    df = df.loc[df['pickup_latitude'].between(40, 42)]
    df = df.loc[df['pickup_longitude'].between(-75, -72)]
    df = df.loc[df['dropoff_latitude'].between(40, 42)]
    df = df.loc[df['dropoff_longitude'].between(-75, -72)]

    # US$ 2.50 is a minimum fare taxi will charge, so we are considering only those fields who are above $2.50
    df = df.loc[df['fare_amount'] > 2.5]
    # Consider records where passengers are present
    df = df.loc[df['passenger_count'] > 0]

    # Drop incorrect/outlier passenger counts
    df = df.loc[df['passenger_count'] <= 6]

    # Create new datetime columns for easy data processing
    df['year'] = df.pickup_datetime.dt.year
    df['month'] = df.pickup_datetime.dt.month
    df['day'] = df.pickup_datetime.dt.day
    df['weekday'] = df.pickup_datetime.dt.weekday
    df['hour'] = df.pickup_datetime.dt.hour

    def haversine_np(lon1, lat1, lon2, lat2):
        """
        This function calculates the great circle distance between two geographical points on the Earth's surface, given their latitude and longitude in decimal degrees. 
        This distance is computed using the Haversine formula, which accounts for the spherical shape of the Earth.    

        """
        lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = np.sin(dlat/2.0)**2 + np.cos(lat1) * \
            np.cos(lat2) * np.sin(dlon/2.0)**2

        c = 2 * np.arcsin(np.sqrt(a))
        km = 6367 * c
        return km

    # Add the new distance column
    df['distance'] = haversine_np(
        df['pickup_longitude'], df['pickup_latitude'], df['dropoff_longitude'], df['dropoff_latitude'])

    df = df.loc[df['distance'] > 0]  # Ignore records where distance is 0

    del df['pickup_datetime']  # Ignore non useful fields

    # Train-test split
    train_data, validation_data, test_data = np.split(
        df.sample(frac=1, random_state=369), [int(.6*len(df)), int(.8*len(df))])
    print(train_data.shape, validation_data.shape, test_data.shape)

    # Upload the datasets to S3
    train_data.to_csv('train.csv', index=False, header=False)
    boto3.Session().resource('s3').Bucket(bucket_name).Object(
        os.path.join(prefix, 'train/train.csv')).upload_file('train.csv')
    validation_data.to_csv('validate.csv', index=False, header=False)
    boto3.Session().resource('s3').Bucket(bucket_name).Object(
        os.path.join(prefix, 'validate/validate.csv')).upload_file('validate.csv')

    # Remove the to be predicted field from test data
    del test_data['fare_amount']

    test_data.to_csv('test.csv', index=False, header=False)
    boto3.Session().resource('s3').Bucket(bucket_name).Object(
        os.path.join(prefix, 'test/test.csv')).upload_file('test.csv')

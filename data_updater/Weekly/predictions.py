from sre import *

#get the data
df = get_data()
date_time = pd.to_datetime(df.pop('Date'), format='%d/%m/%Y')

timestamp_s = date_time.map(pd.Timestamp.timestamp)

#split
column_indices = {name: i for i, name in enumerate(df.columns)}

n = len(df)
train_df = df[0:int(n*0.8)]
val_df = df[int(n*0.8):int(n*0.9)]
test_df = df[int(n*0.90):]

num_features = df.shape[1]

#normalize
train_mean = train_df.mean()
train_std = train_df.std()


train_df = (train_df - train_mean) / train_std
val_df = (val_df - train_mean) / train_std
test_df = (test_df - train_mean) / train_std

#load the trained model
model = tf.keras.models.load_model('saved_model')

#prepare pred_data
in_df = df[-30:]
in_df =  (in_df - train_mean) / train_std
in_df = in_df.astype(float)

in_df = in_df.to_numpy()
in_df = in_df.reshape(1,30,16)

#get predictions
pred_df = model.predict_step(in_df)
pred_df = pd.DataFrame(pred_df[0].numpy(),columns = df.columns)

#denormalize
pred_df = pred_df*train_std + train_mean
pred_vals = pred_df['dailyconfirmed']

#prepare the predictions dataframe with week division
last_date = list(date_time)[-1]
predictions = {}

for i in range(len(pred_vals)):
  key = str(last_date + datetime.timedelta(days=1 + i) ).split(' ')[0]
  predictions[key]  = pred_vals[i]

#create pred_df
pred_df = pd.DataFrame(columns = ['Date','Prediction'])

for i in range(7):
  key = str(date.today() + datetime.timedelta(days=i))
  pred_df.loc[len(pred_df)] = [key,predictions[key]]


# take avg of preds of next week
pred_values = 0
for i in range(7,14):
  key = str(date.today() + datetime.timedelta(days=i))
  pred_values += predictions[key]

avg_pred_values = pred_values/7

pred_df['next_week_predictions_avg'] = avg_pred_values

#append to spredsheet

sheet = get_pred_sheet()

for index, row in pred_df.iterrows():
  sheet.append_row(list(row))

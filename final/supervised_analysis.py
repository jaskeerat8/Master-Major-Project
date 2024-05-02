# Importing Libraries
import joblib
import pandas as pd

# Loading Supervised Model
bit_price = 71333.4847168797
scaler_model_file = "supervised_models/scaler_top_features.pkl"
scaler_model = joblib.load(scaler_model_file)
supervised_model_file = "supervised_models/rf_trained_model_top_features.pkl"
supervised_model = joblib.load(supervised_model_file)

def prediction(transaction, source):
    payload = {"sender_indegree": int(source["sender_indegree"]), "sender_mean_sent": float(source["sender_mean_sent"])*bit_price,
        "sender_total_sent": float(source["sender_total_sent"])*bit_price, "sender_variance_sent": float(source["sender_variance_sent"])*bit_price,
        "receiver_mean_received": float(transaction["receiver_mean_received"])*bit_price, "time_diff": int(source.get("time_difference", 0)),
        "amount_bitcoins": float(source["value"])*bit_price}

    try:
        supervised_df = pd.DataFrame([payload])
        scaled_df = pd.DataFrame(scaler_model.fit_transform(supervised_df), columns=supervised_df.columns)
        illegal_probability = supervised_model.predict_proba(scaled_df)[0][1]
        return illegal_probability
    except:
        print(payload)
        return -0.1


if __name__ == "__main__":
    print("File For making Supervised Prediction")

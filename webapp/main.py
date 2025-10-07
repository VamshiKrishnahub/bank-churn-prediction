import streamlit as st
import requests

st.title("Bank Churn Prediction")

# User input fields
customer_id = st.text_input("Customer ID")
credit_score = st.number_input("Credit Score", min_value=0)
geography = st.selectbox("Geography", ["France", "Spain", "Germany"])
gender = st.selectbox("Gender", ["Male", "Female"])
age = st.number_input("Age", min_value=18)
tenure = st.number_input("Tenure", min_value=0)
balance = st.number_input("Balance", min_value=0.0)
num_of_products = st.number_input("Number of Products", min_value=1)
has_cr_card = st.selectbox("Has Credit Card?", [0, 1])
is_active_member = st.selectbox("Is Active Member?", [0, 1])
estimated_salary = st.number_input("Estimated Salary", min_value=0.0)

# Submit button
if st.button("Predict"):
    input_data = {
        "CustomerID": customer_id,
        "CreditScore": credit_score,
        "Geography": geography,
        "Gender": gender,
        "Age": age,
        "Tenure": tenure,
        "Balance": balance,
        "NumOfProducts": num_of_products,
        "HasCrCard": has_cr_card,
        "IsActiveMember": is_active_member,
        "EstimatedSalary": estimated_salary
    }

    response = requests.post("http://127.0.0.1:8000/predict", json=input_data)
    prediction = response.json()
    st.write("Prediction (0 = Will Not Churn, 1 = Will Churn):", prediction)

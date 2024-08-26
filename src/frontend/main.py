import os
import requests
import streamlit as st

st.title("Canada policy chat bot")
prompt = st.chat_input("Enter your prompt")
messages = st.container(height=300)

if prompt:
    data = {"text": prompt}
    headers = {'Content-type': 'application/json'}
    raw_response = requests.post("http://api-backend/prompt_model", headers=headers, json=data)
    print(raw_response)
    response_content = raw_response.content.decode("utf-8")
    print(response_content)
    response_content = response_content.replace("\n", "<br/>").replace("\"", "")
    messages.chat_message("user").write(prompt)
    messages.chat_message("assistant").write(f"{response_content}")
import requests

url = "https://epitafr.webhook.office.com/webhookb2/62fb41d3-dee7-4cd4-96e7-a790e9beb6e1@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/57c7cd0b3b02420fbb4c522c94f83f82/6f5e8e90-a95f-4824-a2e6-bec60d388e86/V2twSdwDpAgJ2Lvx3TH8DMD___VbMo8Cx9LRiOqoBISF41"

payload = {"text": "🔥 Teams Webhook Test from Python!"}
r = requests.post(url, json=payload)

print("Status:", r.status_code)
print("Response:", r.text)

from urllib import request, parse
import json
def send_message_to_slack(text):

    post = {"text": "{0}".format(text)}

    try:
        json_data = json.dumps(post)
        req = request.Request("https://hooks.slack.com/services/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",
                              data=json_data.encode('ascii'),
                              headers={'Content-Type': 'application/json'})
        resp = request.urlopen(req)
    except Exception as em:
        print("EXCEPTION: " + str(em))

def cli_output(text):
    send_message_to_slack(text)
    print(text)

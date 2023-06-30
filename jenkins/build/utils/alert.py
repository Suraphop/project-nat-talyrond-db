import requests

def line_notify(token,msg):
    try:
        url = 'https://notify-api.line.me/api/notify'
        headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+token}
        r = requests.post(url, headers=headers, data = {'message':msg})
        return r.text
    except Exception as e:
        return e

def slack_notify(webhook_url,msg):
    try:
        payload = '{"text":"%s"}' %msg
        r = requests.post(webhook_url,data=payload)
        return r.text
    except Exception as e:
        return e

if __name__ == "__main__":
    print("must be run with main")
    #value = slack_notify("https://hooks.slack.com/services/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","testing_token") 
    #print(value)

    
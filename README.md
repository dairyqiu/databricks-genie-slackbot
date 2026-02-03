# Databricks Genie Slackbot
Ever wonder how you can deploy a Slackbot in Databricks Apps to connect to your Genie Space?

Well, now you can!

This simple (mostly vibe-coded) app allows you to let your slack users directly query your Databricks Genie Space without ever needing to log in to the Databricks Workspace. You can deploy this locally on your own computer or via Databricks Apps.

Disclaimer: This project is provided as-is for educational and demo purposes only. Please use it at your own risk!

### What is a [Databricks Genie Space](https://docs.databricks.com/aws/en/genie/)

A **Databricks Genie Space** is a shared workspace where teams can use AI-powered apps (called Genies) to chat with their company data and get insights fast. You can ask questions, run queries, or explore reports using natural language, and everything stays secure and governed through Databricks. It’s an easy way for different teams—like sales, support, or engineering—to use AI tools built on their data, all in one place, without needing to be a data expert.

### What is [Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps)?
**Databricks Apps** let you build custom, interactive experiences right inside the Databricks workspace. You can use them to create tools, dashboards, or workflows tailored to your team’s needs, all with full access to your data and notebooks. It’s like giving your workspace superpowers—hook into events, add UI components, and create mini-apps without needing to leave Databricks.

## Key Features
- Allow users to message Genie Space and get responses back!
- Query results should return as a CSV file directly inside of your message
- Messages within a single thread are tracked as a distinct "conversation" in Genie
- Uses Websockets for connection (I chose this mostly due to enterprise security limitations with allowing HTTP connections into their workspace)
- **Performance Optimized**: Implements Databricks Genie API best practices with rate limiting, exponential backoff, and optimized polling
- **Concurrent Processing**: Supports multiple channels and users simultaneously with intelligent queuing
- **Resource Efficient**: Optimized for serverless environments (2 vCPU, 6GB RAM)

## Limitations
- Right now, since it is using the Databricks Apps Oauth permissions, you cannot view the history of your queries in the Databricks UI.


<p align="right">(<a href="#readme-top">back to top</a>)</p>

# Getting Started
For this project, you will need to first set up a Slackbot. Then, deploy the slackbot either to your own local computer or Databricks Apps.

First clone this repository:

```
git clone https://github.com/dairyqiu/databricks-genie-slackbot.git
```

## Setting Up a Slackbot

1. Sign up with a Slack account (or if using your existing one - make sure you have the necessary permissions to create and install Apps)
2. Go to api.slack.com -> Your Apps (top right corner)
3. Create New App -> From a manifest -> Select your Workspace
4. In the JSON tab, paste the following, change the name to what you want to name your bot, then click Next -> Create:

```sh
{
    "display_information": {
        "name": "Genie"
    },
    "features": {
        "app_home": {
            "home_tab_enabled": true,
            "messages_tab_enabled": true,
            "messages_tab_read_only_enabled": false
        },
        "bot_user": {
            "display_name": "Genie",
            "always_online": false
        }
    },
    "oauth_config": {
        "scopes": {
            "bot": [
                "app_mentions:read",
                "channels:read",
                "chat:write",
                "files:read",
                "files:write",
                "im:history",
                "im:read",
                "im:write"
            ]
        }
    },
    "settings": {
        "event_subscriptions": {
            "bot_events": [
                "app_mention",
                "message.im"
            ]
        },
        "interactivity": {
            "is_enabled": true
        },
        "org_deploy_enabled": false,
        "socket_mode_enabled": true,
        "token_rotation_enabled": false
    }
}
```
5. In the left-hand sidebar under **App Home**, scroll to the bottom and click the box "Allow users to send Slash commands and messages from the messages tab". This will simply allow you to directly message the bot in a separate tab.

6. Under **OAuth & Permissions** -> OAuth Tokens, install the app! You should get a token that starts with "xoxb-". This is your SLACK_BOT_TOKEN. Save this as you'll need this later.

7. Finally, under **Basic Information**, scroll to **App-Level Tokens** and click **Generate Token and Scopes**. Name it whatever you want, add the "connections:write:" scope, and generate your token. This is your SLACK_APP_TOKEN and should start with  "xapp-". Save this for later as well.

## Deploying to Databricks Apps

1. We first need to put your SLACK_APP_TOKEN and SLACK_BOT_TOKEN into Databricks Secrets (which you should always use for security reasons or whatever).

You can either go to a Databricks Notebook and paste in this code (replacing with the right values of course):

```
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

scope_name = "<scope-name>" # rename scope - you will need to put this as an env variable later
w.secrets.create_scope(scope=scope_name)

# Replace with app and bot tokens you generated earlier (hopefully)
secrets = {
    "SLACK_APP_TOKEN": "xapp-xxx",
    "SLACK_BOT_TOKEN" : "xoxb-xxx"
}

for key, value in secrets.items():
    w.secrets.put_secret(scope_name, key, string_value=value)

```

OR

Use the databricks CLI.

```
databricks secrets create-scope <scope-name>
```

```
databricks secrets put-secret <scope-name> SLACK_APP_TOKEN
databricks secrets put-secret <scope-name> SLACK_BOT_TOKEN
```

2. We will now need to set up our env variables in [slackbot/app.yaml](slackbot/app.yaml). Databricks Apps uses this file to know what commands to run as well as any env variables you want to inject.

Replace the values under SECRET_SCOPE, GENIE_SPACE_ID, and SHOW_SQL_QUERY (optional)

```
env:
  - name: "SECRET_SCOPE"
    value: "<scope-name>"
  - name: "GENIE_SPACE_ID"
    value: "<your-genie-space-id>"
  - name: "SHOW_SQL_QUERY"
    value: "true" # set to "false" if you don't want to expose SQL queries to end users
```

#### Create your Databricks App

1. Follow this [guide](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/create-custom-app) to setup your App

2. Create your app deployment file. Fill it out with your secrets scope name and Genie Space ID:

```
cp slackbot/app.yaml.example slackbot/app.yaml
```

3. Deploy using Databricks Asset Bundles:

```
databricks bundle deploy && databricks bundle run slackbot_app
```

#### Congrats! Now, you should be able to message your Slackbot in Slack.

## Deploying Locally

1. Create your env file:

```
cp slackbot/env.example slackbot/.env
```

2. Fill out .env file with correct env variables. You will need to [generate a Personal Access Token (PAT)](https://docs.databricks.com/aws/en/dev-tools/auth/pat) in your Databricks workspace.

3. Install packages - you should probably do this in a [virtual env](https://docs.astral.sh/uv/pip/environments/)

```
pip install -r slackbot/requirements.txt
```

4. Deploy the app

```
cd slackbot && python app.py
```

#### Congrats! Now, you should be able to message your Slackbot in Slack.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

  
  
  

<!-- USAGE EXAMPLES -->

## Usage

In Slack, you can either directly talk to your Slackbot under Apps (at the very bottom in the left sidebar), or mention (@) it in a channel (after adding it to the channel).

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## License

This project is licensed under the Apache License 2.0 - see the [LICENSE file](LICENSE) for details.
<p align="right">(<a href="#readme-top">back to top</a>)</p>

  
  
  

<!-- CONTACT -->

## Contact

  

David Qiu - [LinkedIn](https://www.linkedin.com/in/daviqiu/)


  

<p align="right">(<a href="#readme-top">back to top</a>)</p>

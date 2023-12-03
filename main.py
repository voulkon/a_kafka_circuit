import json
from flask import Flask, request, render_template
from confluent_kafka import Producer  # , AdminClient, NewTopic
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
import os
from src.build_config import read_ccloud_config

load_dotenv()
user_info_topic_name = os.getenv("USER_INFO_TOPIC_NAME")

app = Flask(__name__)

producer_config = read_ccloud_config("client.properties")
producer = Producer(producer_config)

admin_config = producer_config.copy()  # Assuming the same config works for AdminClient
admin_client = AdminClient(admin_config)


def create_topic(topic_name):
    """Create a Kafka topic if it does not exist."""
    topics_before = admin_client.list_topics(timeout=10)
    print(f"topics:\t{topics_before.topics}")

    if topic_name not in topics_before.topics:
        topic = NewTopic(topic_name, num_partitions=1, replication_factor=3)
        result = admin_client.create_topics([topic])
        for topic, future in result.items():
            try:
                future.result()  # Wait for operation to complete
                print(f"Topic {topic} created successfully.")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")

    topics_after = admin_client.list_topics(timeout=10)
    print(f"topics:\t{topics_after.topics}")


create_topic(user_info_topic_name)


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        name = request.form.get("name")
        print(f"Name Submitted:\t{name}")
        email = request.form.get("email")
        print(f"Email Submitted:\t{email}")

        user_info = {"name": name, "email": email}
        print(f"user_info prepared:\n{user_info}")
        json_user_info = json.dumps(user_info)

        producer.produce(user_info_topic_name, key=name, value=json_user_info)

        print(f"preparing to flush producer...")
        producer.flush()
        print(f"flush finished")

        return "User info sent to Kafka!"

    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)

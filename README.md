# flink_demo
Flink stateful function - demo

Steps:
docker-compose build --no-cache
docker-compose up -d

Check UI in http://localhost:8041
Stop the Event generation by:
docker stop flink_demo_event-generator_1

Start event generation :
docker start flink_demo_event-generator_1

FROM gcc:latest

WORKDIR /subd

COPY . /subd

RUN g++ main2.cpp sql_parser.cpp server.cpp schema_creator.cpp DualLinkedList.cpp order.cpp request_to_sql.cpp -o main.exe

CMD ["./main.exe"]
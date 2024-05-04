start cmd /k python client.py
start cmd /k python directory_service.py
start cmd /k python locking_service.py
cd fileserverA
start cmd /k python file_serverA.py
cd ../fileserverB
start cmd /k python file_serverB.py
cd ../fileserverC
start cmd /k python file_serverC.py
1. Connect to EMR from terminal
2. First run: `hive --service hiveserver2 &` This command will run hiveserver
3. Second command is: `beeline --incremental=true` to connect to beline
4. And after that run :`!connect jdbc:hive2://localhost:10000` to connect to hive
5. Just press enter when ask for username and password.
6. Now u can use hive and write queries

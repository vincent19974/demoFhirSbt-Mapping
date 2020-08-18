1. Connect to EMR from two diferent terminals
2. In first terminal run: `hive --service hiveserver2 &` This command will run hiveserver
3. In second terminal run: `beeline --incremental=true` to connect to beline
4. And after that `!connect jdbc:hive2://localhost:10000` to connect to hive

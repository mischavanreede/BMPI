Bitcoin core Docker container: https://github.com/ruimarinho/docker-bitcoin-core



Set vm.max_map_count to 262144:
https://stackoverflow.com/questions/42111566/elasticsearch-in-windows-docker-image-vm-max-map-count


To setup elasticsearch password on Windows:
https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-passwords.html
-> Run the example command in Docker CLI and use the generated password for elastic user. Also add this password to the docker-compose.yml
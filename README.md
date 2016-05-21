sudo apt-get install libssl-dev

docker run  -v /srv/thehonestgene_data:/usr/src/app/data -p 127.0.0.1:8000:8000 --name='honestgene-rest'  honestgene-rest -b 0.0.0.0:8000
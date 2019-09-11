# mongodb for single cell pipeline results


### Install

```
$ sudo apt update
$ sudo apt install -y mongodb
```

### Setup

Run Mongo on the machine and create a new user:

```
$ mongo

>>> use admin
>>> db.createUser({user:"dgrewal", pwd:"password", roles:[{role:"root", db:"admin"}]})
>>> exit
```


```
sudo vim /lib/systemd/system/mongodb.service
```

add `--auth` flag to the execstart line.


restart the daemon:
```
sudo systemctl daemon-reload
```


Check if the user creation worked:
```
mongo -u dgrewal -p Password123! --authenticationDatabase admin
```

to enable remote access:
```
 vim /etc/mongodb.conf
```

add the machine IP to the bind_ip, uncomment the line with port no.



##### Optional:
Firewall status can be checked with:

```
ufw status
```
for status. if inactive, run the following commands:

```
ufw allow ssh
ufw allow http
ufw allow https

ufw allow 27017
ufw enable
```



### command reference:

Drop database:

```
use <dbname>
db.dropDatabase()
```


### credentials for azure to load data

fill out and source
```
 export AZURE_KEYVAULT_ACCOUNT=
 export AZURE_STORAGE_ACCOUNT=
 export CLIENT_ID=
 export TENANT_ID=
 export SECRET_KEY=
 export SUBSCRIPTION_ID=
 export RESOURCE_GROUP=
```


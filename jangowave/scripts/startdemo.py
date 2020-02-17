from query.models import FileUpload, AccumuloCluster, Query, UserAuths, Auth, IngestConfiguration


def run(*args):
    admin_user ="admin"
    admin_password="admin"
    auth = Auth()
    auth.auth="TST"
    auth.save()
    democonfig = IngestConfiguration()
    democonfig.name="democonfig"
    democonfig.use_provenance=False
    democonfig.post_location="http://nifi:8181/contentListener"
    democonfig.save()

    instance = "uno"
    zookeepers = "192.168.1.88:2181"
    acc_user = "root"
    acc_pass = "secret"
    for arg in args:
        if arg.startswith("password"):
            split = arg.split("=")
            if len(split) == 2:
                admin_password = split[1]
        if arg.startswith("username"):
            split = arg.split("=")
            if len(split) == 2:
                admin_user = split[1]
        if arg.startswith("instance"):
            split = arg.split("=")
            if len(split) == 2:
                admin_user = split[1]
        if arg.startswith("zookeepers"):
            split = arg.split("=")
            if len(split) == 2:
                zookeepers = split[1]
        if arg.startswith("accuser"):
            split = arg.split("=")
            if len(split) == 2:
                acc_user = split[1]
        if arg.startswith("accpass"):
            split = arg.split("=")
            if len(split) == 2:
                acc_pass = split[1]
    acc = AccumuloCluster()
    acc.instance = instance
    acc.zookeeper = zookeepers
    acc.user = acc_user
    acc.password = acc_pass
    acc.save()
    import pysharkbite
    conf = pysharkbite.Configuration()
    zoo_keeper = pysharkbite.ZookeeperInstance(instance,zookeepers, 1000, conf)
    user = pysharkbite.AuthInfo(acc_user,acc_pass, zoo_keeper.getInstanceId())
    connector = pysharkbite.AccumuloConnector(user, zoo_keeper)
    security_ops = connector.securityOps()
    auths = pysharkbite.Authorizations()
    auths.addAuthorization("MTRCS")
    security_ops.grantAuthorizations(auths,acc_user)

     

    
    
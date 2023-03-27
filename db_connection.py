import yaml
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder

def connect_to_db(config_file, section, ssh=False, local_port=None, ssh_section=None):

    # Read configuration information from file
    config = yaml.safe_load(open(config_file, 'r'))
    
    if local_port:
        config[section]['port'] = local_port
        print(f"local_port: {local_port}")
        print(f"config[section]['port']: {config[section]['port']}")

    if ssh:
        ssh_config = config[ssh_section]
        server = SSHTunnelForwarder(
            ssh_address_or_host=(ssh_config['host'], ssh_config['port']),
            ssh_username=ssh_config['user'],
            ssh_password=ssh_config['password'],
            remote_bind_address=(config[section]['docker_host'], config[section]['port'])
        )
        server.start()

        

        url = '{type}://{user}:{password}@localhost:{port}/{db_name}'.format(type=config[section]['type'], user=config[section]['user'],
            password=config[section]['password'], db_name=config[section]['db_name'],
            port=server.local_bind_port)
        print(f"SQLAlchemy url: {url}")
        engine = create_engine(url)

        return engine.connect()

    else:
        
        url = '{type}://{user}:{password}@{host}:{port}/{db_name}'.format(**config[section])
        print(f"SQLAlchemy url: {url}")
        engine = create_engine(url)

        return engine.connect()
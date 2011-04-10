import ConfigParser
class TestInput():

    @staticmethod
    def master():
        master = TestInput.servers()[0]
        return master

    @staticmethod
    def servers():
        ips = []
        config = ConfigParser.ConfigParser()
        config.read('server.conf')
        sections = config.sections()
        for section in sections:
            if section == 'servers':
                options = config.options(section)
                for option in options:
                    ips.append(config.get(section,option))
        return ips

    @staticmethod
    def get_username():
        config = ConfigParser.ConfigParser()
        config.read('server.conf')
        sections = config.sections()
        for section in sections:
            if section == 'membase':
                options = config.options(section)
                for option in options:
                    if option == 'username':
                        return config.get(section,option)
        return ''

    @staticmethod
    def get_password():
        config = ConfigParser.ConfigParser()
        config.read('server.conf')
        sections = config.sections()
        for section in sections:
            if section == 'membase':
                options = config.options(section)
                for option in options:
                    if option == 'password':
                        return config.get(section,option)
        return ''

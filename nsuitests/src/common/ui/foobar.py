class Foobar(object):
    
    name = ''
    
    def greet(self,name):
        self.name = name
        return "hi"
    
    def bye(self):
        if self.name == '':
            raise KeyError("we did not meet yet")
        return "bye {0}".format(self.name)
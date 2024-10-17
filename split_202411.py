import os.path


class Splitter():
    def __init__(self,file_ini):
        self.file_ini = file_ini
        self.path_out = os.path.dirname(file_ini)
        self.prename = 'O'
        self.resolution = 'fr'
        self.area = 'bal'
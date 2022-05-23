# TODO - logger class untested
class Logger:
    def __init__(self):
        self.file = open("rynner_logs.txt", "w")

    def info(self, message):
        self.file.write(message + '\n')
        self.file.flush()

    def __del__(self):
        self.file.close()

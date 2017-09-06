import mmap
import csv
import email
import sys
import time
from mongoengine import connect, Document, fields

csv.field_size_limit(sys.maxsize)

INSERT_BLOCK_SIZE = 50000
MONGODB_NAME = 'enron'

connect(MONGODB_NAME)


class EmployeeMessages(Document):
    email_to = fields.ListField(fields.EmailField())
    name_to = fields.ListField(fields.StringField())
    email_from = fields.EmailField()
    name_from = fields.StringField()
    message = fields.StringField()
    file = fields.StringField()


class ReadAndInsertFile(object):
    def __init__(self, path_file):
        self.path_file = path_file

    def parse_message(self, row):
        message = email.message_from_string(row['message'])
        return {
            'email_to': message.get('To', '').split(','),
            'name_to': message.get('X-To', '').split(','),
            'email_from': message.get('From'),
            'name_from': message.get('X-From'),
            'message': message.get_payload(),
            'file': row['file'],
        }

    def inserts(self, messages, blocks_size=INSERT_BLOCK_SIZE):
        total_messages = len(messages)
        blocks = int(total_messages / blocks_size) + 1
        init = 0

        EmployeeMessages.drop_collection()

        start_time_4 = time.time()

        for i in range(0, blocks):
            start_time = time.time()
            EmployeeMessages.objects.insert(messages[init:init + blocks_size])
            print("--- Block %s/%s -> %s seconds ---" % (i + 1, blocks, time.time() - start_time))
            init = init + blocks_size

        print("--- Inserted %s blocks: %s seconds ---" % (blocks, time.time() - start_time_4))

        count_messages = self.get_total_messages()
        print("--- Inserted %s messages" % count_messages)

    def get_total_messages(self):
        return EmployeeMessages.objects.count()

    def read_file(self):
        try:
            with open(self.path_file, 'r+b') as csv_file:
                start_time = time.time()
                reader = csv.DictReader(csv_file, delimiter=',')
                print("--- Readed file: %s seconds ---" % (time.time() - start_time))

                start_time_2 = time.time()
                messages = [EmployeeMessages(**self.parse_message(row)) for row in reader]
                print("--- Parsed rows to Email Message: %s seconds ---" % (time.time() - start_time_2))

                return messages
        except IOError as e:
            print e
        return None

    def word_count(self):
        start_time = time.time()
        map_f = """
            function() {
                emit(this.name_from, this.message.split(" ").length);
            }
        """

        reduce_f = """
            function(key, values) {
                return Array.sum(values);
            }
        """

        results = EmployeeMessages.objects.map_reduce(map_f, reduce_f, "word_count")
        results = list(results)
        print("--- Map reduce: %s seconds ---" % (time.time() - start_time))
        print("--- Employee with most words written: %s ---" % results[0].value)

    def start(self):
        start_time = time.time()
        messages = self.read_file()
        total_messages = len(messages)
        print("--- Total messages: %s ---" % total_messages)
        self.inserts(messages)
        self.word_count()
        print("--- TOTAL TIME: %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
    read_file = ReadAndInsertFile('emails.csv')
    read_file.start()

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRCustomerSpend(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.map_customer_orders, reducer=self.reduce_customer_orders),
            MRStep(mapper=self.map_spend_per_customer, reducer=self.reduce_spend_per_customer)
        ]
        
    def map_customer_orders(self, _, line):
        (customer_id, item_id, price) = line.split(',')
        yield customer_id, float(price)

    def reduce_customer_orders(self, customer_id, price_list):
        yield customer_id, sum(price_list)

    def map_spend_per_customer(self, customer_id, total_price):
        yield '%04.02f'%float(total_price), customer_id
    
    def reduce_spend_per_customer(self, total_price, customer_ids):
        for customer_id in customer_ids:
            yield customer_id, total_price


if __name__ == '__main__':
    MRCustomerSpend.run()

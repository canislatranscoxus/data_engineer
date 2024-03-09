import typing
import apache_beam as beam
class CastFields( beam.DoFn ):
    def process(self, element ):
        #print( 'element: ', type( element ), element )
        #row = element

        row = beam.Row(
              pay_id            = int( element.pay_id )
            , order_id          = element.order_id
            , amount            = element.amount
            , status            = element.status.upper()
            , payment_method    = element.payment_method.upper()
            , payment_timestamp = element.payment_timestamp )
        return [ row ]
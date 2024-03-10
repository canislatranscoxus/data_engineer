import typing
import apache_beam as beam
class CastFields_noti( beam.DoFn ):
    def process(self, element ):
        #print( 'element: ', type( element ), element )
        #row = element

        row = beam.Row(
              pay_id            = int( element.pay_id )
            , order_id          = element.order_id
            , status            = element.status.upper()
            , amount            = element.amount
        )
        return [ row ]
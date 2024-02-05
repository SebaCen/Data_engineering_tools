if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data = data[data['passenger_count'] > 0]
    data = data[data['trip_distance'] > 0]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    data.rename(columns={'VendorID':'vendor_id', 
                        'RatecodeID':'ratecode_id',
                        'PULocationID':'pulocation_id', 
                        'DOLocationID':'dolocation_id'},
                         inplace=True)
    data['vendor_id'].value_counts()
    
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() == 0, 'Hay conductores sin pasajeros'

   
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['trip_distance'].isin([0]).sum() == 0, 'Hay viajes sin distancia recorrida'

@test
def test_output(output, *args) -> None:
    
    assert output['vendor_id'].isin([1,2]).sum() != 0, 'vendor_id con valor incorrecto'
import pywren_ibm_cloud as pywren
import time,datetime, pickle, pytz

bucket_name='practica-sd-mp'
result_file='result.txt'

N_SLAVES = 10
if N_SLAVES <=0: 
    N_SLAVES=1
elif N_SLAVES>100:
    N_SLAVES=100

def master(id, x, ibm_cos):
    write_permission_list = []
    done=0
    ibm_cos.put_object(Bucket=bucket_name,Key='result.txt')
    time.sleep(2)

    first=True
    ultimaActualitzacio=pytz.utc.localize(datetime.datetime.now())
    while done<N_SLAVES:
        ara=ibm_cos.get_object(Bucket=bucket_name,Key=result_file)['LastModified']
        if(ultimaActualitzacio<ara or first):
            aux=done
            #Since it's in, it can't get out without giving permission to one slave
            while aux==done:
                if first:
                    first=False
                list_files=ibm_cos.list_objects(Bucket=bucket_name)['Contents']
                slaves_bucket=[]
                ultimaActualitzacio=ara
                for fitxer in list_files:
                    if(fitxer['Key'][0:2] == 'p_'):
                        slaves_bucket.append({"NomFitxer":fitxer['Key'], "DataCreacio":fitxer['LastModified']})
                
                if(len(slaves_bucket)!=0):
                    #order for time of creation
                    slaves_bucket=sorted(slaves_bucket,key=lambda k:k['DataCreacio'])
                    #Deleting p_ so it remains write_id
                    ibm_cos.put_object(Bucket=bucket_name,Key=slaves_bucket[0]['NomFitxer'][2:])
                    write_permission_list.append(slaves_bucket[0]['NomFitxer'][8:])
                    ibm_cos.delete_object(Bucket=bucket_name,Key=slaves_bucket[0]['NomFitxer'])
                    done+=1
        time.sleep(2)
    return write_permission_list





def slave(id, x, ibm_cos):
    ibm_cos.put_object(Bucket=bucket_name,Key=f'p_write_{id}')
    fet=True
    while fet:
        try:
            #check if you have permission (write_{id} exists)
            ibm_cos.get_object(Bucket=bucket_name, Key=f'write_{id}')
            ibm_cos.delete_object(Bucket=bucket_name, Key=f'write_{id}')
            fet=False
            #try for checking if it's empty
            try:
                result=ibm_cos.get_object(Bucket=bucket_name, Key='result.txt')['Body'].read()
                content=pickle.loads(result)
                content.append(f'{id}')
            except Exception:
                content=[]
                content.append(f'{id}')
            
            ibm_cos.put_object(Bucket=bucket_name, Key=result_file, Body= pickle.dumps(content))         
            return
        except Exception:
            pass
        time.sleep(2)
    return


if __name__ == '__main__':
    pw = pywren.ibm_cf_executor()
    start_time = time.time()
    pw.call_async(master, 0)
    pw.map(slave, range(N_SLAVES))
    write_permission_list = pw.get_result()
    elapsed_time = time.time() - start_time

    ibm_cos = pw.internal_storage.get_client()

    try:
        print(f'The list from the master: {write_permission_list[0]}')
        result = ibm_cos.get_object(Bucket=bucket_name, Key=result_file)['Body'].read()
        list_result = pickle.loads(result)
        print(f'The result.txt file: {list_result}')

        if (write_permission_list[0] == list_result):
            print("Equal lists: True")
        else:
            print("Equal lists: False")
    except Exception:
        print('Error in the result.txt file or the return of master')



    list_files = ibm_cos.list_objects(Bucket=bucket_name)['Contents']
    # Deleting all remaining files (including some that should be deleted previously)
    created_files = ['pwrite', 'write_', result_file]
    for elem in list_files:
        eliminat = elem['Key']
        if (elem['Key'] in s for s in created_files):
            eliminat = elem['Key']
            ibm_cos.delete_object(Bucket=bucket_name, Key=elem['Key'])



    print(f"Temps d'execució en segons: {elapsed_time} s")

import pywren_ibm_cloud as pywren
import time,datetime
#from datetime import datetime
import pickle


bucket_name='practica-sd-mp'
#someone_in=False
N_SLAVES = 5
if N_SLAVES <=0: N_SLAVES=1

def master(id, x, ibm_cos):
    write_permission_list = []

    done=0
    ibm_cos.put_object(Bucket=bucket_name,Key='result.txt')
    time.sleep(1)
    ultimaActualitzacio=ibm_cos.get_object(Bucket=bucket_name,Key='result.txt')['LastModified']
    first=True
    while done<N_SLAVES:
        ara=ibm_cos.get_object(Bucket=bucket_name,Key='result.txt')['LastModified']
        if(ultimaActualitzacio<ara or first):
            first=False
            list_files=ibm_cos.list_objects(Bucket=bucket_name, Marker='p_write_')['Contents']
            slaves_bucket=[]
            for fitxer in list_files:
                slaves_bucket.append({"NomFitxer":fitxer['Key'], "DataCreacio":fitxer['LastModified']})
            #ordenem per la data de modificacio
            slaves_bucket=sorted(slaves_bucket,key=lambda k:k['DataCreacio'])
            #fiquem el write_{id}, per fer aixo borrem el p_ del nom
            ibm_cos.put_object(Bucket=bucket_name,Key=slaves_bucket[0]['NomFitxer'][2:])
            ibm_cos.delete_object(Bucket=bucket_name,Key='ninguExecutant')
            ibm_cos.delete_object(Bucket=bucket_name,Key=slaves_bucket[0]['NomFitxer'])
            write_permission_list.append(id)
            done+=1
      
        time.sleep(2)
    return write_permission_list
 # 1. monitor COS bucket each X seconds
 # 2. List all "p_write_{id}" files
 # 3. Order objects by time of creation
 # 4. Pop first object of the list "p_write_{id}"
 # 5. Write empty "write_{id}" object into COS
 # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
 # 7. Monitor "result.json" object each X seconds until it is updated
 # 8. Delete from COS “write_{id}”
 # 8. Back to step 1 until no "p_write_{id}" objects in the bucket




def slave(id, x, ibm_cos):
    ibm_cos.put_object(Bucket=bucket_name,Key=f'p_write_{id}')
    while True:
        try:
            ibm_cos.get_object(Bucket=bucket_name, Key=f'write_{id}')
            #ibm_cos.put_object(Bucket=bucket_name,Key=f'{id}')
            #ibm_cos.delete_object(bucket_name, Key=f'write_{id}')
            try:
                content=pickle.loads(ibm_cos.get_object(Bucket=bucket_name, Key='result.txt')['Body'].read())
                content.append(f'{id}')
            except Exception:
                content=f'{id}'
            ibm_cos.put_object(Bucket=bucket_name, Key='result.txt', Body= pickle.dumps(content))
            return
        except Exception:
            pass
        time.sleep(5)

    
    #cos.put_object(bucket_name,'buit','No hi ha ningú al result.txt')
 # 1. Write empty "p_write_{id}" object into COS
 # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
 # 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
 # 4. Finish
 # No need to return anything


if __name__ == '__main__':
    pw = pywren.ibm_cf_executor()
    pw.call_async(master, 0)
    pw.map(slave, range(N_SLAVES))
    write_permission_list = pw.get_result()
    print(write_permission_list)


    #print(pickle.loads(cos.get_object(bucket_name,'result.txt')))
 # Get result.txt
 # check if content of result.txt == write_permission_list
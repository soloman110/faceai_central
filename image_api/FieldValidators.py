from rest_framework import serializers

'''

https://stackoverflow.com/questions/44085153/how-to-validate-a-json-object-in-django

[
   {
      "id":"52",
      "metainfo":{
         "emotion":"netual",
         "age":23,
         "gender":"male"
      }
   }
]
'''

class MetainfoSerializer(serializers.Serializer):
    '''
    age = serializers.IntegerField(required=True, min_value=1) #null=True
    emotion = serializers.CharField(required=True, max_length=50)
    gender = serializers.CharField(required=True)
    '''
    id = serializers.CharField(required=True)  # null=True
    #metainfo = serializers.ListField(required=True, allow_empty=False)  # null=True

class MetainfoValidator(serializers.Serializer):
    #metainfos = serializers.JSONField()

    token = serializers.CharField(required=True)
    metainfos = serializers.CharField(required=True)

    #metainfos = serializers.ListField(child=MetainfoSerializer())


class ImageinfoValidator(serializers.Serializer):
    ftpid = serializers.IntegerField(required=False, min_value=1) #null=True
    imglist = serializers.CharField(required=True)

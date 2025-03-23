from senlerpy import Senler, methods


api = Senler('18348769d7e14e145824825dd480bb7f6693d770bd936351')

#190688644, 203462729, 212606175, 229010532,
      #191199326, 215681031, 212764328, 189780349, 185092197, 218497525, 219027468,
      #218338256, 156841006, 121657214, 213219340, 219154949, 219027204, 172445157, 210984679
records = api(
    methods.Subscribers.stat_subscribe,
    date_from='2025-03-01',
    date_to='2025-03-02',
    vk_group_id=191199326,
    count=100
)

items = records['items']


print(len(items))
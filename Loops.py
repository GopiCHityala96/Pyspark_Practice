# for i in range(1,5):
#     if i == 3:
#         break
#     else:
#         print(i)
#
# i = 0
# while i < 5:
#
#     if i == 3:
#         i += 1
#         continue
#
#     print(i)
# #     i += 1
# #
# age = int(input("enter your age:"))
# if age == 18:
#      print("is elisible for vote")
# elif age > 18:
#     print("vote elisible for vote")
# else:
#     print("not elisble for the vote")


n = int(input("enter your number:"))
if n%2==0:
    print("even number")
elif n%2!=0:
    print("odd number")

while True:
    age = int(input("Enter your age: "))
    if age < 0:
        print("Age cannot be negative. Try again.")
        continue
    if age == 18:
        print("Is eligible for vote")
    elif age > 18:
        print("Eligible for vote")
    else:
        print("Not eligible for the vote")
    break  # Exit the loop after valid input and response





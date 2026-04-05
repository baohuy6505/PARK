# Bước 1: Build file JAR bằng Maven
FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests

# Bước 2: Chạy file JAR bằng Java 21
FROM eclipse-temurin:21-jre
WORKDIR /app
# Copy chính xác file app.jar (tên đã đặt trong pom.xml)
COPY --from=build /app/target/app.jar app.jar

EXPOSE 8080

# THAY ĐỔI QUAN TRỌNG NHẤT: Dùng -cp thay vì -jar
ENTRYPOINT ["java", "-cp", "app.jar", "com.example.GateMain"]
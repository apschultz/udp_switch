# Compiler and flags
CC      := gcc
CFLAGS  := -std=gnu11 -g -O2 -Wall -Wextra
LDFLAGS := -levent -levent_pthreads -lpthread -ljansson

# File and directory settings
SRC_DIR   := src
BUILD_DIR := build
SRC       := $(wildcard $(SRC_DIR)/*.c)
OBJ       := $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(SRC))
DEPS      := $(OBJ:.o=.d)
TARGET    := $(BUILD_DIR)/udp_switch

udp_switch: $(TARGET)

all: udp_switch



# Link final binary
$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR):
	mkdir -p $@

# Compile .c to .o with dependency file
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -MMD -c $< -o $@

# Include generated dependency files
-include $(DEPS)

# Clean rule
clean:
	rm -rf $(BUILD_DIR)

.PHONY: all udp_switch clean


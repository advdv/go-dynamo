variable "project" {}
variable "version" {}
variable "owner" {}

output "env" {
  value = "${data.template_file.env.vars}"
}

resource "random_id" "id" { byte_length = 4 }
data "template_file" "p" {
  template = "$${i}-$${p}$${v}-$${o}"
  vars {
    i = "${random_id.id.hex}"
    v = "${replace(lower(var.version),"/[^a-zA-Z0-9]/", "")}"
    p = "${replace(lower(var.project),"/[^a-zA-Z0-9]/", "")}"
    o = "${replace(replace(lower(var.owner),"/[^a-zA-Z0-9]/", ""), "/(.{0,5})(.*)/", "$1")}"
  }
}

data "template_file" "env" {
  template = ""
  vars {
    "TEST_TABLE_NAME" = "${aws_dynamodb_table.game_scores.name}"
  }
}

resource "aws_dynamodb_table" "game_scores" {
  name = "${data.template_file.p.rendered}-game-scores"
  read_capacity = 1
  write_capacity = 1
  hash_key = "GameTitle"
  range_key = "UserId"

  attribute {
    name = "GameTitle"
    type = "S"
  }

  attribute {
    name = "UserId"
    type = "S"
  }

  attribute {
    name = "TopScore"
    type = "N"
  }

  global_secondary_index {
    name               = "GameTitleIndex"
    hash_key           = "GameTitle"
    range_key          = "TopScore"
    write_capacity     = 1
    read_capacity      = 1
    projection_type    = "INCLUDE"
    non_key_attributes = ["UserId"]
  }
}

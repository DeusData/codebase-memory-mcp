extends "res://actors/base.gd"
signal ghost_hit
func ghost_attack():
    emit_signal("ghost_hit")

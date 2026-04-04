class_name Player
const BaseAlias = preload("res://actors/base.gd")
const ReceiverClass = preload("res://actors/receiver.gd")
extends BaseAlias
signal hit
const Weapon = preload("res://actors/weapon.gd")
var WeaponRel = load("weapon.gd")
const Scene = preload("res://actors/player.tscn")
func attack():
    emit_signal("hit")
    self.hit.emit()
    hit.connect(_on_hit)
    var r = ReceiverClass.new()
    r.hit.connect(_on_receiver_hit)
    r.hit.emit()
    helper()

func helper():
    pass
